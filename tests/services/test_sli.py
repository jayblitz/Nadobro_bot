"""SLI module: labelled latency + counters with bounded cardinality."""
from __future__ import annotations

import pytest

from src.nadobro.services import sli


@pytest.fixture(autouse=True)
def _reset():
    sli.reset()
    yield
    sli.reset()


def test_record_latency_creates_labelled_series():
    sli.record_latency("strategy.cycle", 12.5, user_id=42, network="mainnet", strategy="dgrid")
    sli.record_latency("strategy.cycle", 30.0, user_id=42, network="mainnet", strategy="dgrid")
    snap = sli.snapshot(metric_prefix="strategy.")
    keys = [v for v in snap.values() if v.get("count")]
    assert len(keys) == 1
    entry = keys[0]
    assert entry["count"] == 2
    assert entry["labels"]["user_id"] == "42"
    assert entry["p95_ms"] >= entry["p50_ms"]


def test_timed_span_records_outcome_label():
    with sli.timed_span("op", user_id=1):
        pass
    with pytest.raises(RuntimeError):
        with sli.timed_span("op", user_id=1):
            raise RuntimeError("boom")
    snap = sli.snapshot(metric_prefix="op")
    outcomes = sorted({v["labels"].get("outcome") for v in snap.values()})
    assert outcomes == ["error", "success"]


def test_increment_counter():
    sli.increment("orders.placed", user_id=7, outcome="success")
    sli.increment("orders.placed", user_id=7, outcome="success", delta=3)
    sli.increment("orders.placed", user_id=8, outcome="success")
    snap = sli.snapshot(metric_prefix="orders.placed")
    counts = sorted(v["counter"] for v in snap.values())
    assert counts == [1, 4]


def test_per_user_filter():
    sli.record_latency("strategy.cycle", 1.0, user_id=1)
    sli.record_latency("strategy.cycle", 2.0, user_id=2)
    user1 = sli.metrics_for_user(1)
    assert len(user1) == 1
    only = next(iter(user1.values()))
    assert only["labels"]["user_id"] == "1"


def test_cardinality_cap_evicts_lru(monkeypatch):
    import importlib
    monkeypatch.setenv("NADO_SLI_MAX_SERIES", "5")
    from src.nadobro.services import sli as sli_module

    importlib.reload(sli_module)
    for uid in range(10):
        sli_module.record_latency("strategy.cycle", 1.0, user_id=uid)
    snap = sli_module.snapshot()
    assert len([v for v in snap.values() if v.get("count")]) <= 5
