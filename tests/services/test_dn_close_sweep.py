"""DN spot-leg verify-and-sweep on stop.

Regression: the manual DN close fired ONE spot sell with no verification, so a
partial fill / rejection left the KBTC leg in the wallet — the user stayed
directional after hitting Stop. ``_close_spot_leg_with_sweep`` now retries until
the leg is provably flat (or only dust remains) and reports any real residual.
"""
from __future__ import annotations

import src.nadobro.trading.trade_service as ts


class _FakeRO:
    def __init__(self, size: float, mid: float, pid: int = 1):
        self.size = size
        self.mid = mid
        self.pid = pid

    def get_balance(self):
        return {"balances": {self.pid: self.size}}

    def get_market_price(self, _pid):
        return {"mid": self.mid}


def _sweep(monkeypatch, ro, fake_sell):
    monkeypatch.setattr(ts, "get_user_readonly_client", lambda *a, **k: ro)
    monkeypatch.setattr(ts, "MIN_TRADE_SIZE_USD", 1.0)
    monkeypatch.setattr(ts, "execute_spot_market_order", fake_sell)
    return ts._close_spot_leg_with_sweep(
        1, "BTC", ro.pid, "KBTC", network="mainnet",
        slippage_pct=1.0, source="dn", strategy_session_id=None,
    )


def test_flat_after_one_sell(monkeypatch):
    ro = _FakeRO(size=1.0, mid=100.0)
    sells = []

    def fake_sell(telegram_id, asset, size, is_buy, **kw):
        sells.append(size)
        ro.size = 0.0                      # fully sold
        return {"success": True}

    res = _sweep(monkeypatch, ro, fake_sell)
    assert res["success"] is True
    assert res["rounds"] == 1
    assert sells == [1.0]


def test_retries_then_reports_residual(monkeypatch):
    ro = _FakeRO(size=1.0, mid=100.0)
    sells = []

    def fake_sell(telegram_id, asset, size, is_buy, **kw):
        sells.append(size)                 # "succeeds" but the balance never drops
        return {"success": True}

    res = _sweep(monkeypatch, ro, fake_sell)
    assert res["success"] is False         # exposure surfaced, not hidden
    assert res["rounds"] == 3              # retried up to max_rounds
    assert len(sells) == 3
    assert res["residual_value_usd"] == 100.0


def test_sub_min_dust_is_treated_as_flat(monkeypatch):
    ro = _FakeRO(size=0.005, mid=100.0)    # ~$0.50 < $1 minimum
    sells = []

    def fake_sell(*a, **k):
        sells.append(a)
        return {"success": True}

    res = _sweep(monkeypatch, ro, fake_sell)
    assert res["success"] is True
    assert res.get("dust") is True
    assert sells == []                     # never tried to sell un-sellable dust


def test_success_is_verified_not_sell_return(monkeypatch):
    """A sell that REPORTS failure but actually flattened the balance still
    counts as closed — the verdict is the post-trade balance, not the response."""
    ro = _FakeRO(size=1.0, mid=100.0)

    def fake_sell(telegram_id, asset, size, is_buy, **kw):
        ro.size = 0.0                      # actually flat
        return {"success": False, "error": "status unknown"}

    res = _sweep(monkeypatch, ro, fake_sell)
    assert res["success"] is True
    assert res["spot_size"] == 0.0
