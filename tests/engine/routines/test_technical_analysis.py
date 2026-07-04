import asyncio

from src.nadobro.engine.routines import technical_analysis as ta


def _c(closes):
    return [{"close": c, "high": c + 1, "low": c - 1} for c in closes]


def test_uptrend_indicators():
    out = asyncio.run(ta.run("P", _c([float(i) for i in range(1, 40)])))
    assert out["trend"] == "up"
    assert out["ema_fast"] > out["ema_slow"]
    assert out["rsi"] is not None and out["rsi"] > 50
    assert out["atr"] is not None and out["volatility"] is not None


def test_downtrend():
    out = asyncio.run(ta.run("P", _c([float(i) for i in range(40, 1, -1)])))
    assert out["trend"] == "down"
    assert out["rsi"] < 50


def test_flat_and_insufficient_data():
    out = asyncio.run(ta.run("P", _c([100.0, 100.0])))
    assert out["trend"] == "flat"
    assert out["rsi"] is None  # need more than rsi_period closes


def test_macd_sign_tracks_momentum():
    up = [100 + i * 0.5 for i in range(60)]
    down = [100 - i * 0.5 for i in range(60)]
    m_up = ta.macd(up)
    m_down = ta.macd(down)
    assert m_up is not None and m_up["macd"] > 0      # fast EMA above slow
    assert m_down is not None and m_down["macd"] < 0
    # histogram present and finite
    assert isinstance(m_up["histogram"], float)


def test_macd_none_on_short_history():
    assert ta.macd([1.0, 2.0, 3.0]) is None
    assert ta.macd(list(range(30))) is None  # < slow(26)+signal(9)


def test_bollinger_bands_and_pctb():
    up = [100 + i * 0.5 for i in range(40)]
    b = ta.bollinger(up)
    assert b is not None
    assert b["lower"] < b["middle"] < b["upper"]
    assert 0.5 < b["pct_b"] <= 1.2          # price rides the upper half in an uptrend
    # middle band is the SMA of the window
    assert abs(ta.bollinger(list(range(1, 21)))["middle"] - 10.5) < 1e-9


def test_bollinger_none_on_short_history():
    assert ta.bollinger([1.0, 2.0]) is None


def test_run_includes_macd_and_bollinger():
    out = asyncio.run(ta.run("P", _c([float(i) for i in range(1, 60)])))
    assert out["macd"] is not None and "histogram" in out["macd"]
    assert out["bollinger"] is not None and "pct_b" in out["bollinger"]
