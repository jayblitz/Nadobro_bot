"""Overlay runtime wiring — _maybe_apply_overlay mutates the mapped configs
for MM strategies and no-ops elsewhere. Pure of any real DB/venue (mocked)."""
from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from src.nadobro.strategy import engine_runtime as er


class _FakeClient:
    """get_candlesticks returns an uptrend for every timeframe."""
    def get_candlesticks(self, product_id, timeframe, limit, max_time=None):
        return [
            {"close": 100 + i * 0.4, "high": 100 + i * 0.4 + 1, "low": 100 + i * 0.4 - 1, "volume": 10}
            for i in range(80)
        ]


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    from src.nadobro.strategy import market_features as mf
    mf.reset_cache()
    er._OVERLAY_FUNDING_CACHE.clear()
    monkeypatch.setenv("NADO_SIGNAL_OVERLAY", "1")
    # Persistence is best-effort; stub it so no DB is needed.
    import src.nadobro.models.database as db
    monkeypatch.setattr(db, "insert_overlay_signal", lambda row: 1, raising=False)
    yield
    mf.reset_cache()
    er._OVERLAY_FUNDING_CACHE.clear()


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


def test_overlay_rail_sl_never_widens_past_user_stop():
    """The uptrend fixture reads as a trend (signal SL = base x 1.3), but the
    rail barrier must stay clamped at the user's configured stop."""
    cfg = {"order_amount_quote": Decimal("500"), "directional_bias": 0.0}
    state = {"strategy": "mid", "strategy_session_id": 1, "sl_pct": 0.5, "tp_pct": 1.0}
    asyncio.run(er._maybe_apply_overlay(
        7, "mainnet", "mid", "BTC", 2, cfg, state, client=_FakeClient(), mid=131.6,
    ))
    assert state["overlay_sl_pct"] <= 0.5


def test_overlay_rail_stays_disarmed_when_user_has_no_sl_tp():
    """A user who runs without a session SL/TP must not get rail barriers armed
    by the overlay (the 10% drawdown cap is the backstop)."""
    cfg = {"order_amount_quote": Decimal("500"), "directional_bias": 0.0}
    state = {"strategy": "mid", "strategy_session_id": 1, "sl_pct": 0.0, "tp_pct": 0.0}
    asyncio.run(er._maybe_apply_overlay(
        7, "mainnet", "mid", "BTC", 2, cfg, state, client=_FakeClient(), mid=131.6,
    ))
    assert "overlay_sl_pct" not in state
    assert "overlay_tp_pct" not in state


def test_overlay_persist_throttled_while_signal_holds(monkeypatch):
    """A steady signal must not insert a row every tick — only on an applied
    change or the heartbeat."""
    inserts = {"n": 0}
    import src.nadobro.models.database as db
    monkeypatch.setattr(
        db, "insert_overlay_signal", lambda row: inserts.update(n=inserts["n"] + 1) or 1,
        raising=False,
    )
    cfg_factory = lambda: {"order_amount_quote": Decimal("500"), "directional_bias": 0.0}
    state = {"strategy": "mid", "strategy_session_id": 1, "sl_pct": 0.5, "tp_pct": 1.0}
    for _ in range(3):
        asyncio.run(er._maybe_apply_overlay(
            7, "mainnet", "mid", "BTC", 2, cfg_factory(), state,
            client=_FakeClient(), mid=131.6,
        ))
    assert inserts["n"] == 1
    # The stable factors are still applied to every cycle's fresh configs.
    cfg = cfg_factory()
    asyncio.run(er._maybe_apply_overlay(
        7, "mainnet", "mid", "BTC", 2, cfg, state, client=_FakeClient(), mid=131.6,
    ))
    assert cfg["directional_bias"] != 0.0


def test_overlay_funding_read_is_cached_across_ticks():
    calls = {"funding": 0}

    class _ClientWithFunding(_FakeClient):
        def get_perp_funding_rates(self, product_ids):
            calls["funding"] += 1
            return {int(product_ids[0]): {"funding_rate": 0.0012}}

    state = {"strategy": "mid", "strategy_session_id": 1, "sl_pct": 0.5, "tp_pct": 1.0}
    for _ in range(3):
        asyncio.run(er._maybe_apply_overlay(
            7, "mainnet", "mid", "BTC", 2,
            {"order_amount_quote": Decimal("500"), "directional_bias": 0.0}, state,
            client=_ClientWithFunding(), mid=131.6,
        ))
    assert calls["funding"] == 1


def test_overlay_fetches_funding_without_breaking():
    calls = {"funding": 0}

    class _ClientWithFunding(_FakeClient):
        def get_perp_funding_rates(self, product_ids):
            calls["funding"] += 1
            return {int(product_ids[0]): {"funding_rate": 0.0012}}

    cfg = {"order_amount_quote": Decimal("500"), "directional_bias": 0.0,
           "spread_bid_pct": Decimal("0.0005"), "spread_ask_pct": Decimal("0.0005")}
    state = {"strategy": "mid", "strategy_session_id": 1, "sl_pct": 0.5, "tp_pct": 1.0}
    asyncio.run(er._maybe_apply_overlay(
        7, "mainnet", "mid", "BTC", 2, cfg, state, client=_ClientWithFunding(), mid=131.6,
    ))
    # Funding was fetched and the overlay still applied (bias set).
    assert calls["funding"] == 1
    assert cfg["directional_bias"] != 0.0


def test_overlay_survives_funding_fetch_failure():
    class _ClientFundingBoom(_FakeClient):
        def get_perp_funding_rates(self, product_ids):
            raise RuntimeError("indexer down")

    cfg = {"order_amount_quote": Decimal("500"), "directional_bias": 0.0}
    state = {"strategy": "mid", "strategy_session_id": 1, "sl_pct": 0.5, "tp_pct": 1.0}
    # Must not raise; overlay still applies from candle features.
    asyncio.run(er._maybe_apply_overlay(
        7, "mainnet", "mid", "BTC", 2, cfg, state, client=_ClientFundingBoom(), mid=131.6,
    ))
    assert cfg["directional_bias"] != 0.0


def test_funding_flag_fires_with_held_long_position(monkeypatch):
    """End-to-end: a held long + positive funding -> the persisted signal
    carries a funding carry-cost risk (position side read from inventory)."""
    class _Hold:
        net_amount_base = 0.05          # long
    class _Inv:
        def get(self, *a, **k):
            return _Hold()
    class _Ctrl:
        inventory = _Inv()
        trading_pair = "BTC-PERP"
        id = "mid:7:mainnet"

    class _ClientFunding(_FakeClient):
        def get_perp_funding_rates(self, product_ids):
            return {int(product_ids[0]): {"funding_rate": 0.0015}}   # longs pay

    monkeypatch.setitem(er.RUNTIME._controllers, (7, "mainnet", "mid"), _Ctrl())
    captured = {}
    import src.nadobro.models.database as db
    monkeypatch.setattr(db, "insert_overlay_signal", lambda row: captured.update(row) or 1, raising=False)

    cfg = {"order_amount_quote": Decimal("500"), "directional_bias": 0.0}
    state = {"strategy": "mid", "strategy_session_id": 1, "sl_pct": 0.5, "tp_pct": 1.0}
    try:
        asyncio.run(er._maybe_apply_overlay(
            7, "mainnet", "mid", "BTC", 2, cfg, state, client=_ClientFunding(), mid=131.6,
        ))
    finally:
        er.RUNTIME._controllers.pop((7, "mainnet", "mid"), None)

    risks = captured.get("risks_json") or []
    assert any("Funding" in r and "paying" in r for r in risks)
