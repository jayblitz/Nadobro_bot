"""Backtester candle ingest — produce an in-order stream of OHLC candles for
the backtester engine.

Three sources are supported:

* ``candles_from_ohlc`` — explicit OHLC rows (e.g. a venue candle feed).
* ``candles_from_prices`` — a price path (one close per step); each step becomes
  a candle whose high/low straddle the move so resting limits can be crossed.
* ``resample_trades_csv`` — resample a Nado trade/fill export (the repo ships
  ``f14288_*_trades_*.csv``) into OHLC candles at a fixed interval.

Everything is :class:`~decimal.Decimal` to match the engine adapter + inventory,
so there is no float drift between the sim and the strategies under test.

Implemented in Phase 5 (backtester).
"""
from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Iterable, List, Optional, Sequence

from src.nadobro.engine.types import _dec


@dataclass(frozen=True)
class Candle:
    """One OHLC bar. ``ts`` is epoch seconds at the bar's OPEN."""

    ts: float
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal = Decimal(0)

    @property
    def mid(self) -> Decimal:
        """Representative price for the bar — the close (what a controller sees
        when it ticks on this bar)."""
        return self.close


def candles_from_ohlc(rows: Iterable[dict], *, interval_s: float = 3600.0) -> List[Candle]:
    """Build candles from explicit OHLC dict rows.

    Each row needs ``open/high/low/close`` (or ``o/h/l/c``); ``ts``/``time`` and
    ``volume``/``v`` are optional. Rows are returned sorted by timestamp.
    """
    out: List[Candle] = []
    for i, row in enumerate(rows):
        def _g(*keys: str, default: object = None) -> object:
            for k in keys:
                if k in row and row[k] is not None:
                    return row[k]
            return default

        ts = _g("ts", "time", "timestamp", default=i * interval_s)
        o = _dec(_g("open", "o"))
        h = _dec(_g("high", "h"))
        lo = _dec(_g("low", "l"))
        c = _dec(_g("close", "c"))
        v = _dec(_g("volume", "v", default=0))
        out.append(Candle(float(ts), o, h, lo, c, v))
    out.sort(key=lambda x: x.ts)
    return out


def candles_from_prices(
    prices: Sequence[object],
    *,
    start_ts: float = 0.0,
    interval_s: float = 3600.0,
    wick_pct: Decimal = Decimal("0.0"),
) -> List[Candle]:
    """Turn a close-price path into candles. ``wick_pct`` (fraction) widens each
    bar's high/low around the open..close range so resting limit orders just
    inside the move can be crossed. With ``wick_pct=0`` the bar spans exactly
    open..close (deterministic, no look-through beyond the realized move)."""
    out: List[Candle] = []
    prev = _dec(prices[0]) if prices else Decimal(0)
    wick = _dec(wick_pct)
    for i, p in enumerate(prices):
        close = _dec(p)
        open_ = prev
        hi = max(open_, close)
        lo = min(open_, close)
        if wick > 0:
            hi = hi * (Decimal(1) + wick)
            lo = lo * (Decimal(1) - wick)
        out.append(Candle(start_ts + i * interval_s, open_, hi, lo, close, Decimal(0)))
        prev = close
    return out


def _parse_epoch(raw: str) -> Optional[float]:
    raw = (raw or "").strip()
    if not raw:
        return None
    # ISO-8601 (the CSV's ``Time`` column), tolerating a trailing ``Z``.
    try:
        txt = raw.replace("Z", "+00:00")
        return datetime.fromisoformat(txt).replace(tzinfo=timezone.utc).timestamp() \
            if "+" not in txt else datetime.fromisoformat(txt).timestamp()
    except ValueError:
        pass
    # Bare epoch (s or ms).
    try:
        val = float(raw)
        return val / 1000.0 if val > 1e11 else val
    except ValueError:
        return None


def resample_trades_csv(
    path: str,
    *,
    interval_s: float = 3600.0,
    market: Optional[str] = None,
) -> List[Candle]:
    """Resample a Nado trade/fill export into OHLC candles at ``interval_s``.

    Columns used: ``Time`` (ISO), ``Market``, ``Price``, ``Amount``. When
    ``market`` is given only that symbol's prints are used (the export can mix
    markets). Buckets are floored to ``interval_s``; OHLC is built from the
    chronological prints in each bucket and volume is the summed notional.
    """
    buckets: dict[int, dict] = {}
    with open(path, newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            if market is not None and str(row.get("Market", "")).strip().upper() != market.upper():
                continue
            ts = _parse_epoch(str(row.get("Time", "")))
            if ts is None:
                continue
            try:
                price = _dec(row.get("Price"))
                amount = _dec(row.get("Amount") or 0)
            except Exception:  # noqa: BLE001  # policy: degrade-ok(skip malformed CSV print; offline candle parse, no live money)
                continue
            if price <= 0:
                continue
            bucket = int(ts // interval_s) * int(interval_s)
            b = buckets.get(bucket)
            if b is None:
                buckets[bucket] = {
                    "open": price, "high": price, "low": price, "close": price,
                    "vol": amount * price, "first": ts, "last": ts,
                }
            else:
                b["high"] = max(b["high"], price)
                b["low"] = min(b["low"], price)
                b["vol"] += amount * price
                if ts >= b["last"]:
                    b["last"] = ts
                    b["close"] = price
                if ts < b["first"]:
                    b["first"] = ts
                    b["open"] = price
    out = [
        Candle(float(bucket), b["open"], b["high"], b["low"], b["close"], b["vol"])
        for bucket, b in sorted(buckets.items())
    ]
    return out
