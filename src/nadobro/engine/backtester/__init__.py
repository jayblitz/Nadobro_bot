"""Event-driven backtester — runs the SAME strategy controllers the live engine
builds against historical candles through a cost-aware simulated venue (fees +
funding + slippage), and reports NET-of-fees performance.

Public API::

    from src.nadobro.engine.backtester import (
        Candle, candles_from_prices, candles_from_ohlc, resample_trades_csv,
        SimCosts, SimMeta, SimNadoAdapter,
        run_backtest, BacktestEngine, BacktestReport,
    )

Implemented in Phase 5.
"""
from src.nadobro.engine.backtester.candle_ingest import (
    Candle,
    candles_from_ohlc,
    candles_from_prices,
    resample_trades_csv,
)
from src.nadobro.engine.backtester.engine import BacktestEngine, run_backtest
from src.nadobro.engine.backtester.executor_sim import SimCosts, SimMeta, SimNadoAdapter
from src.nadobro.engine.backtester.report import BacktestReport

__all__ = [
    "Candle",
    "candles_from_ohlc",
    "candles_from_prices",
    "resample_trades_csv",
    "SimCosts",
    "SimMeta",
    "SimNadoAdapter",
    "run_backtest",
    "BacktestEngine",
    "BacktestReport",
]
