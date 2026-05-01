import pandas as pd

from src.nadobro.studio.conditions import evaluate
from src.nadobro.studio.intent import Condition


def _candles():
    close = list(range(1, 41))
    return pd.DataFrame({
        "time": pd.date_range("2026-01-01", periods=len(close), freq="h", tz="UTC"),
        "open": close,
        "high": [x + 1 for x in close],
        "low": [x - 1 for x in close],
        "close": close,
        "volume": [100] * len(close),
    })


def test_ema_condition_true():
    assert evaluate(Condition(indicator="EMA", period=9, operator=">", value=20), _candles())


def test_price_cross_dedup_last_two_bars():
    df = _candles()
    assert evaluate(Condition(indicator="PRICE", operator="crosses_above", value=39), df)
    assert not evaluate(Condition(indicator="PRICE", operator="crosses_above", value=20), df)


def test_macd_and_bbands_return_bool():
    df = _candles()
    assert isinstance(evaluate(Condition(indicator="MACD", operator=">=", value=0), df), bool)
    assert isinstance(evaluate(Condition(indicator="BBANDS", period=20, operator=">", value=0), df), bool)
