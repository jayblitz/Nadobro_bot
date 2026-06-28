"""Backtester reporting — equity curve, drawdown, and NET-of-fees PnL.

The headline metric is ``net_pnl`` = realized − fees + funding + final unrealized.
This is deliberately the number the live system under-reported (see the audit's
SLTP-GROSS / DN-PNL-FEES findings): a backtest that ignored fees/funding would
declare a bleeding strategy profitable, so the harness leads with the net figure.

Implemented in Phase 5 (backtester).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import List


def _max_drawdown(curve: List[Decimal]) -> Decimal:
    """Largest peak-to-trough drop in the equity curve (absolute quote)."""
    peak = None
    worst = Decimal(0)
    for v in curve:
        if peak is None or v > peak:
            peak = v
        dd = (peak - v) if peak is not None else Decimal(0)
        if dd > worst:
            worst = dd
    return worst


@dataclass
class BacktestReport:
    strategy: str
    bars: int = 0
    realized_pnl: Decimal = Decimal(0)
    fees: Decimal = Decimal(0)
    funding: Decimal = Decimal(0)
    final_unrealized: Decimal = Decimal(0)
    orders_placed: int = 0
    fills: int = 0
    equity_curve: List[Decimal] = field(default_factory=list)

    @property
    def gross_pnl(self) -> Decimal:
        """PnL ignoring fees/funding (realized price PnL + open unrealized)."""
        return self.realized_pnl + self.final_unrealized

    @property
    def net_pnl(self) -> Decimal:
        """The honest number: realized − fees + funding + open unrealized."""
        return self.realized_pnl - self.fees + self.funding + self.final_unrealized

    @property
    def max_drawdown(self) -> Decimal:
        return _max_drawdown(self.equity_curve)

    @property
    def fee_drag(self) -> Decimal:
        """How much fees ate into the gross result (>0 means fees hurt)."""
        return self.fees

    def summary(self) -> str:
        return (
            f"[{self.strategy}] bars={self.bars} fills={self.fills} "
            f"orders={self.orders_placed}\n"
            f"  gross PnL : {self.gross_pnl:+.4f}\n"
            f"  fees      : -{self.fees:.4f}\n"
            f"  funding   : {self.funding:+.4f}\n"
            f"  unrealized: {self.final_unrealized:+.4f}\n"
            f"  NET PnL   : {self.net_pnl:+.4f}\n"
            f"  max DD    : {self.max_drawdown:.4f}"
        )
