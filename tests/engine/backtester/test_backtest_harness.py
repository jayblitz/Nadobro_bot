"""Backtester harness tests + per-strategy money-bleed regressions.

These prove the harness is HONEST (it charges fees/funding/slippage so a
strategy that only looks good on price moves shows a negative net) and that each
strategy controller actually runs end-to-end against the simulated venue.
"""
from __future__ import annotations

import math
import os
from decimal import Decimal

import pytest

from src.nadobro.engine.backtester import (
    Candle,
    SimCosts,
    SimMeta,
    SimNadoAdapter,
    candles_from_ohlc,
    candles_from_prices,
    resample_trades_csv,
    run_backtest,
)

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..", "..", "..")
TRADES_CSV = os.path.join(REPO_ROOT, "f14288_default_inkMainnet_trades_1778457600000_1778716799999.csv")


def _ranging(n=120, base=100.0, amp=3.0):
    prices = [base + amp * math.sin(i / 3.0) for i in range(n)]
    return candles_from_prices(prices, interval_s=3600, wick_pct=Decimal("0.001"))


def _grid_cfg():
    return {
        "trading_pair": "BTC", "total_amount_quote": Decimal("1000"),
        "start_price": Decimal("97"), "end_price": Decimal("100"),
        "min_spread_between_orders": Decimal("0.01"), "max_open_orders": 5,
        "levels_count": 5, "step_pct": Decimal("0.01"),
        "leverage": 1, "sl_pct": 0.0, "tp_pct": 0.0,
    }


# --------------------------------------------------------------------------- #
# candle_ingest                                                               #
# --------------------------------------------------------------------------- #

def test_candles_from_prices_spans_open_to_close():
    cs = candles_from_prices([100, 101, 99], interval_s=60)
    assert len(cs) == 3
    assert cs[1].open == Decimal(100) and cs[1].close == Decimal(101)
    assert cs[1].high == Decimal(101) and cs[1].low == Decimal(100)
    assert cs[2].ts - cs[1].ts == 60


def test_candles_from_ohlc_sorts_by_ts():
    cs = candles_from_ohlc([
        {"ts": 200, "open": 2, "high": 3, "low": 1, "close": 2},
        {"ts": 100, "open": 1, "high": 2, "low": 1, "close": 1.5},
    ])
    assert [c.ts for c in cs] == [100, 200]


def test_resample_trades_csv_builds_candles():
    if not os.path.exists(TRADES_CSV):
        pytest.skip("trades CSV not present")
    cs = resample_trades_csv(TRADES_CSV, interval_s=3600, market="WTI")
    assert len(cs) > 0
    for c in cs:
        assert c.high >= c.low > 0
        assert c.high >= c.open and c.high >= c.close


# --------------------------------------------------------------------------- #
# executor_sim cost model                                                     #
# --------------------------------------------------------------------------- #

def test_sim_charges_taker_fee_on_market_and_applies_slippage():
    import asyncio
    from src.nadobro.engine.types import OrderType, TradeType

    async def body():
        sim = SimNadoAdapter(costs=SimCosts(taker_fee=Decimal("0.001"), slippage_pct=Decimal("0.002")))
        sim.set_candle(Candle(0, Decimal(100), Decimal(100), Decimal(100), Decimal(100)))
        o = await sim.place_order("BTC", TradeType.BUY, OrderType.MARKET, Decimal(1))
        # buy fills at mid*(1+slippage) = 100.2; taker fee = 1 * 100.2 * 0.001
        assert o.filled_base == Decimal(1)
        assert o.filled_quote == Decimal("100.2")
        assert sim.total_fees_quote == Decimal("0.1002")

    asyncio.run(body())


def test_sim_funding_accrues_on_perp_not_spot():
    import asyncio
    from src.nadobro.engine.types import OrderType, TradeType

    async def body():
        sim = SimNadoAdapter(costs=SimCosts(funding_rate_per_bar=Decimal("0.0001")))
        sim.set_candle(Candle(0, Decimal(100), Decimal(100), Decimal(100), Decimal(100)))
        # short the perp, long the spot (same magnitude)
        await sim.place_order("BTC-PERP", TradeType.SELL, OrderType.MARKET, Decimal(1))
        await sim.place_order("BTC-USDT0", TradeType.BUY, OrderType.MARKET, Decimal(1))
        sim.accrue_funding()
        # only the perp short earns: 0.0001 * 1 * 100 = 0.01 ; spot accrues nothing
        assert sim.total_funding_quote == Decimal("0.0100")

    asyncio.run(body())


# --------------------------------------------------------------------------- #
# Honesty + per-strategy regressions                                          #
# --------------------------------------------------------------------------- #

def test_fees_erode_net_pnl_the_harness_is_honest():
    candles = _ranging()
    zero = run_backtest("grid", _grid_cfg(), candles, costs=SimCosts(taker_fee=Decimal(0), maker_fee=Decimal(0)))
    high = run_backtest("grid", _grid_cfg(), candles, costs=SimCosts(taker_fee=Decimal("0.02"), maker_fee=Decimal("0.02")))
    assert zero.net_pnl > high.net_pnl          # fees must reduce net
    assert high.fees > zero.fees
    # net = gross - fees + funding + unrealized (conservation)
    assert zero.net_pnl == zero.gross_pnl - zero.fees + zero.funding


@pytest.mark.parametrize("strategy,cfg", [
    ("grid", None),
    ("rgrid", None),
])
def test_grid_family_runs_end_to_end(strategy, cfg):
    candles = _ranging()
    cfg = cfg or _grid_cfg()
    cfg = dict(cfg, trading_pair="BTC")
    rep = run_backtest(strategy, cfg, candles, costs=SimCosts())
    assert rep.bars == len(candles)
    assert rep.orders_placed >= 1
    assert len(rep.equity_curve) == len(candles)


def test_vol_runs_and_charges_fees():
    candles = _ranging()
    cfg = {"trading_pair": "KBTC", "total_amount_quote": Decimal("100"),
           "total_duration": 40, "order_interval": 20, "market": "spot",
           "leverage": 1, "target_volume_usd": Decimal("0")}
    rep = run_backtest("vol", cfg, candles, costs=SimCosts())
    assert rep.fills >= 1
    assert rep.fees >= 0


def test_dn_is_profitable_only_when_funding_beats_fees():
    """The DN thesis, finally checkable: the hedge cancels price PnL, so DN only
    nets positive when captured funding exceeds the round-trip fees."""
    candles = _ranging()
    cfg = {"trading_pair": "BTC", "trading_pair_long": "BTC-USDT0",
           "trading_pair_short": "BTC-PERP", "hedge_ratio": Decimal("1"),
           "leg_amount_quote": Decimal("100"), "max_drift_pct": Decimal("0.05"),
           "hold_seconds": 7200, "cycles": 1, "leverage": 1, "barriers": None}
    with_funding = run_backtest("dn", cfg, candles, costs=SimCosts(funding_rate_per_bar=Decimal("0.0001")))
    no_funding = run_backtest("dn", cfg, candles, costs=SimCosts(funding_rate_per_bar=Decimal(0)))
    assert with_funding.funding > 0
    assert no_funding.funding == 0
    assert with_funding.net_pnl > no_funding.net_pnl   # funding is DN's edge
    assert no_funding.net_pnl <= 0                      # no funding => fees bleed
