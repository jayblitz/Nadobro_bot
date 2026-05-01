"""Candle retrieval and short-lived cache for Strategy Studio indicators."""

from __future__ import annotations

import time

import pandas as pd

from src.nadobro.config import get_product_id

_CACHE: dict[tuple[str, str, str], tuple[float, pd.DataFrame]] = {}
_TF_SECONDS = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}


def _ttl(timeframe: str) -> int:
    return _TF_SECONDS.get(timeframe, 3600)


def candles_to_frame(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows or [])
    if df.empty:
        return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])
    for col in ("open", "high", "low", "close", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    if "time" in df:
        df["time"] = pd.to_datetime(df["time"], unit="s", errors="coerce", utc=True).fillna(
            pd.to_datetime(df["time"], errors="coerce", utc=True)
        )
    return df.dropna(subset=["open", "high", "low", "close"]).sort_values("time").reset_index(drop=True)


def fetch_candles(client, symbol: str, timeframe: str = "1h", limit: int = 200, network: str = "mainnet") -> pd.DataFrame:
    key = (network, symbol.upper(), timeframe)
    cached = _CACHE.get(key)
    now = time.time()
    if cached and now - cached[0] < _ttl(timeframe):
        return cached[1].copy()
    product_id = get_product_id(symbol, network=network, client=client)
    if product_id is None:
        return candles_to_frame([])
    rows = []
    if hasattr(client, "get_candlesticks"):
        rows = client.get_candlesticks(int(product_id), timeframe=timeframe, limit=limit)
    frame = candles_to_frame(rows)
    _CACHE[key] = (now, frame)
    return frame.copy()
