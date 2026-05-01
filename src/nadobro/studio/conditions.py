"""Indicator calculation and condition evaluation for Strategy Studio."""

from __future__ import annotations

import operator as op

import pandas as pd

from src.nadobro.studio.intent import Condition

_OPS = {
    "<": op.lt,
    "<=": op.le,
    ">": op.gt,
    ">=": op.ge,
    "==": lambda a, b: abs(float(a) - float(b)) < 1e-9,
}


def _series(condition: Condition, candles: pd.DataFrame, ctx: dict) -> pd.Series:
    indicator = condition.indicator.upper()
    close = candles["close"].astype(float)
    high = candles["high"].astype(float)
    low = candles["low"].astype(float)
    volume = candles["volume"].astype(float) if "volume" in candles else pd.Series([0] * len(candles))
    period = int(condition.period or 14)
    params = condition.params or {}
    if indicator == "PRICE":
        return close
    if indicator == "FUNDING":
        return pd.Series([float(ctx.get("funding_rate") or 0)] * len(candles))
    if indicator == "RSI":
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(period).mean()
        loss = (-delta.clip(upper=0)).rolling(period).mean()
        rs = gain / loss.replace(0, pd.NA)
        return 100 - (100 / (1 + rs))
    if indicator == "EMA":
        return close.ewm(span=period, adjust=False).mean()
    if indicator == "SMA":
        return close.rolling(period).mean()
    if indicator == "MACD":
        fast = close.ewm(span=int(params.get("fast", 12)), adjust=False).mean()
        slow = close.ewm(span=int(params.get("slow", 26)), adjust=False).mean()
        macd = fast - slow
        signal = macd.ewm(span=int(params.get("signal", 9)), adjust=False).mean()
        return macd - signal
    if indicator == "BBANDS":
        band = str(params.get("band", "lower")).lower()
        mean = close.rolling(period).mean()
        std = close.rolling(period).std()
        if band == "upper":
            return mean + float(params.get("std", 2)) * std
        if band == "middle":
            return mean
        return mean - float(params.get("std", 2)) * std
    if indicator == "ATR":
        prev_close = close.shift(1)
        tr = pd.concat([(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
        return tr.rolling(period).mean()
    if indicator == "STOCH":
        lowest = low.rolling(period).min()
        highest = high.rolling(period).max()
        return ((close - lowest) / (highest - lowest).replace(0, pd.NA)) * 100
    if indicator == "VWAP":
        typical = (high + low + close) / 3
        return (typical * volume).cumsum() / volume.replace(0, pd.NA).cumsum()
    raise ValueError(f"Unsupported indicator {condition.indicator}")


def evaluate(condition: Condition, candles: pd.DataFrame, ctx: dict | None = None) -> bool:
    ctx = ctx or {}
    if candles is None or len(candles) < 2:
        return False
    values = _series(condition, candles, ctx).dropna()
    if len(values) < 2:
        return False
    current = float(values.iloc[-1])
    previous = float(values.iloc[-2])
    target = float(condition.value)
    if condition.operator == "crosses_above":
        return previous <= target < current
    if condition.operator == "crosses_below":
        return previous >= target > current
    return bool(_OPS[condition.operator](current, target))


def describe_current(condition: Condition, candles: pd.DataFrame, ctx: dict | None = None) -> str:
    values = _series(condition, candles, ctx or {}).dropna()
    current = float(values.iloc[-1]) if len(values) else 0.0
    return f"{condition.indicator} {condition.timeframe} {condition.operator} {condition.value:g} — currently {current:.2f}"
