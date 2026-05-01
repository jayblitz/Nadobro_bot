"""Lightweight Strategy Studio backtesting with simple fees and slippage."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pandas as pd

from src.nadobro.config import EST_FEE_RATE
from src.nadobro.studio.conditions import evaluate
from src.nadobro.studio.intent import TradingIntent


@dataclass
class BacktestReport:
    pnl_usd: float
    max_drawdown_pct: float
    hit_rate_pct: float
    trades: int
    equity_curve: list[float]

    def ascii_curve(self, width: int = 32) -> str:
        if not self.equity_curve:
            return ""
        points = self.equity_curve[-width:]
        lo, hi = min(points), max(points)
        chars = "▁▂▃▄▅▆▇█"
        if hi == lo:
            return chars[0] * len(points)
        return "".join(chars[int((p - lo) / (hi - lo) * (len(chars) - 1))] for p in points)


def backtest(
    intent: TradingIntent,
    candles: pd.DataFrame,
    start: datetime | None = None,
    end: datetime | None = None,
    starting_equity: float = 1000.0,
    slippage_bps: float = 5.0,
) -> BacktestReport:
    """Walk bars with next-bar fills, estimated fees, and a basic fixed slippage model."""
    if candles is None or candles.empty:
        return BacktestReport(0.0, 0.0, 0.0, 0, [])
    df = candles.copy()
    if start is not None and "time" in df:
        df = df[df["time"] >= pd.Timestamp(start)]
    if end is not None and "time" in df:
        df = df[df["time"] <= pd.Timestamp(end)]
    if len(df) < 3:
        return BacktestReport(0.0, 0.0, 0.0, 0, [])

    equity = starting_equity
    curve = [equity]
    wins = 0
    trades = 0
    side = 1 if intent.action == "buy" else -1
    slip = float(slippage_bps) / 10000.0
    qty = float(intent.quantity.value) if intent.quantity and intent.quantity.type == "contracts" else 1.0
    for idx in range(1, len(df) - 1):
        window = df.iloc[: idx + 1]
        if intent.conditions and not all(evaluate(c, window, {}) for c in intent.conditions):
            curve.append(equity)
            continue
        entry = float(df.iloc[idx + 1]["open"]) * (1 + slip * side)
        exit_price = float(df.iloc[min(idx + 2, len(df) - 1)]["open"]) * (1 - slip * side)
        gross = (exit_price - entry) * qty * side
        fees = (abs(entry * qty) + abs(exit_price * qty)) * float(EST_FEE_RATE)
        pnl = gross - fees
        equity += pnl
        trades += 1
        wins += 1 if pnl > 0 else 0
        curve.append(equity)
        # V1 simulates one fill for market/limit intents and repeated opportunities for condition tests.
        if not intent.conditions:
            break
    peak = curve[0] if curve else starting_equity
    max_dd = 0.0
    for value in curve:
        peak = max(peak, value)
        if peak:
            max_dd = max(max_dd, (peak - value) / peak * 100.0)
    return BacktestReport(
        pnl_usd=equity - starting_equity,
        max_drawdown_pct=max_dd,
        hit_rate_pct=(wins / trades * 100.0) if trades else 0.0,
        trades=trades,
        equity_curve=curve,
    )
