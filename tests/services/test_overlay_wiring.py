"""Overlay runtime wiring — _maybe_apply_overlay mutates the mapped configs
for MM strategies and no-ops elsewhere. Pure of any real DB/venue (mocked)."""
from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from src.nadobro.services import engine_runtime as er


class _FakeClient:
    """get_candlesticks returns an uptrend for every timeframe."""
    def get_candlesticks(self, product_id, timeframe, limit, max_time=None):
        return [
            {"close": 100 + i * 0.4, "high": 100 + i * 0.4 + 1, "low": 100 + i * 0.4 - 1, "volume": 10}
            for i in range(80)
        ]


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    from src.nadobro.services import market_features as mf
    mf.reset_cache()
    monkeypatch.setenv("NADO_SIGNAL_OVERLAY", "1")
    # Persistence is best-effort; stub it so no DB is needed.
    import src.nadobro.models.database as db
    monkeypatch.setattr(db, "insert_overlay_signal", lambda row: 1, raising=False)
    yield
    mf.reset_cache()


def test_overlay_steers_mid_bias_and_size():
    cfg = {
        "order_amount_quote": Decimal("500"),
        "spread_bid_pct": Decimal("0.0005"),
        "spread_ask_pct": Decimal("0.0005"),
        "directional_bias": 0.0,
    }
    state = {"strategy": "mid", "strategy_session_id": 1, "sl_pct": 0.5, "tp_pct": 1.0}
    asyncio.run(er._maybe_apply_overlay(
        7, "mainnet", "mid", "BTC", 2, cfg, state, client=_FakeClient(), mid=131.6,
    ))
    # Uptrend -> long bias applied, size scaled up, spread widened.
    assert cfg["directional_bias"] > 0.2
    assert Decimal(str(cfg["order_amount_quote"])) > Decimal("500")


def test_overlay_noop_for_non_mm_strategy():
    cfg = {"leg_amount_quote": Decimal("50")}
    before = dict(cfg)
    state = {"strategy": "dn"}
    asyncio.run(er._maybe_apply_overlay(
        7, "mainnet", "dn", "BTC", 2, cfg, state, client=_FakeClient(), mid=100.0,
    ))
    assert cfg == before   # DN is not an overlay strategy


def test_overlay_noop_when_flag_off(monkeypatch):
    monkeypatch.setenv("NADO_SIGNAL_OVERLAY", "0")
    cfg = {"order_amount_quote": Decimal("500"), "directional_bias": 0.0}
    before = dict(cfg)
    state = {"strategy": "mid"}
    asyncio.run(er._maybe_apply_overlay(
        7, "mainnet", "mid", "BTC", 2, cfg, state, client=_FakeClient(), mid=100.0,
    ))
    assert cfg == before


def test_overlay_noop_without_client_candles():
    cfg = {"order_amount_quote": Decimal("500"), "directional_bias": 0.0}
    before = dict(cfg)
    state = {"strategy": "grid"}
    # client without get_candlesticks -> overlay bails cleanly.
    asyncio.run(er._maybe_apply_overlay(
        7, "mainnet", "grid", "BTC", 2, cfg, state, client=object(), mid=100.0,
    ))
    assert cfg == before


def test_overlay_writes_barrier_state_for_rail():
    cfg = {"order_amount_quote": Decimal("500"), "directional_bias": 0.0}
    state = {"strategy": "mid", "strategy_session_id": 1, "sl_pct": 0.5, "tp_pct": 1.0}
    asyncio.run(er._maybe_apply_overlay(
        7, "mainnet", "mid", "BTC", 2, cfg, state, client=_FakeClient(), mid=131.6,
    ))
    # Regime-adjusted barriers surfaced to state for the session rail.
    assert "overlay_sl_pct" in state and state["overlay_sl_pct"] > 0
    assert "overlay_tp_pct" in state and state["overlay_tp_pct"] > 0
