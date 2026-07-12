"""Multi-timeframe feature gathering: caching, stale-ok, and per-TF reduction."""
from __future__ import annotations

import pytest

from src.nadobro.strategy import market_features as mf


@pytest.fixture(autouse=True)
def _clear():
    mf.reset_cache()
    yield
    mf.reset_cache()


def _series(base: float, step: float, n: int = 60):
    return [{"close": base + i * step, "high": base + i * step + 1, "low": base + i * step - 1} for i in range(n)]


def test_multi_tf_shape_and_trend():
    def fetch(pid, tf, limit):
        return _series(100.0, 0.4)

    feats = mf.multi_tf_features(fetch, "mainnet", 2, now=1000.0)
    assert set(feats) == set(mf.DEFAULT_TIMEFRAMES)
    assert feats["1h"]["trend"] == "up"
    assert feats["1h"]["rsi"] > 50
    assert feats["1h"]["variance_ratio"] is not None


def test_ttl_cache_dedupes_then_refetches():
    calls = []

    def fetch(pid, tf, limit):
        calls.append(tf)
        return _series(100.0, 0.4)

    mf.multi_tf_features(fetch, "mainnet", 2, now=1000.0)   # 3 tf fetches
    mf.multi_tf_features(fetch, "mainnet", 2, now=1005.0)   # within TTL -> cached
    assert len(calls) == 3
    mf.multi_tf_features(fetch, "mainnet", 2, now=100000.0)  # past every TTL
    assert len(calls) == 6


def test_fetch_failure_serves_last_good():
    def ok(pid, tf, limit):
        return _series(100.0, 0.4)

    mf.multi_tf_features(ok, "mainnet", 2, now=1000.0)

    def boom(pid, tf, limit):
        raise RuntimeError("indexer down")

    feats = mf.multi_tf_features(boom, "mainnet", 2, now=1001.0)
    assert feats["1h"]["candles"] == 60          # stale value, not blank


def test_empty_fetch_is_safe():
    feats = mf.multi_tf_features(lambda p, t, l: [], "mainnet", 2, now=1000.0)
    assert feats["1h"]["trend"] == "flat"
    assert feats["1h"]["rsi"] is None
    assert feats["1h"]["candles"] == 0


def test_downtrend_features():
    feats = mf.multi_tf_features(lambda p, t, l: _series(150.0, -0.4), "mainnet", 3, now=1000.0)
    assert feats["4h"]["trend"] == "down"
    assert feats["4h"]["rsi"] < 50
    assert feats["4h"]["macd_hist"] is not None


def test_macd_hist_is_price_normalized():
    """The histogram is emitted as a FRACTION of the last close so the fusion
    vote is comparable across a $100k asset and a sub-cent token."""
    cheap = mf.compute_tf_features(_series(0.0001, 0.0001 * 0.004))
    pricey = mf.compute_tf_features(_series(100000.0, 100000.0 * 0.004))
    assert cheap["macd_hist"] is not None and pricey["macd_hist"] is not None
    # Identical relative moves -> (nearly) identical normalized histograms.
    assert cheap["macd_hist"] == pytest.approx(pricey["macd_hist"], rel=1e-6)
    # And the value is a small fraction, not a price-scale number.
    assert abs(pricey["macd_hist"]) < 0.05
