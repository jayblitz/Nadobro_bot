"""Phase 5 — per-product daily loss budget + exposure registry tests.

Locks in:

  1. record_pnl_snapshot / record_realized_pnl accumulate correctly and
     combine into get_daily_pnl.
  2. check_budget: ok / soft_stopped / hard_stopped thresholds.
  3. Hard stop persists a cooldown that survives subsequent calls.
  4. resolve_budget_thresholds scales defaults with notional.
  5. Exposure registry: record + aggregate + conflicting-side detection +
     stale-row exclusion.
  6. mm_bot integration: a hard-stopped product returns
     action="risk_budget_hard_stopped" and places no orders.
  7. mm_bot integration: budget OK proceeds normally.
"""
from __future__ import annotations

import sys
import time
import types

import pytest

from src.nadobro.services import risk_budget as rb


# ---------------------------------------------------------------------------
# In-memory bot_state backing so tests don't touch a real DB.
# ---------------------------------------------------------------------------

class _Store:
    def __init__(self):
        self.data: dict = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value):
        self.data[key] = value

    def delete(self, key):
        self.data.pop(key, None)


@pytest.fixture
def store(monkeypatch):
    s = _Store()
    monkeypatch.setattr(rb, "get_bot_state", lambda k: s.get(k))
    monkeypatch.setattr(rb, "set_bot_state", lambda k, v: s.set(k, v))

    def _fake_execute(sql, params=()):
        if "DELETE FROM bot_state" in sql and params:
            s.delete(params[0])
        return None

    monkeypatch.setattr(rb, "execute", _fake_execute)
    return s


# ---------------------------------------------------------------------------
# Daily loss budget
# ---------------------------------------------------------------------------

class TestDailyBudget:
    def test_pnl_snapshot_and_realized_combine(self, store):
        rb.record_realized_pnl(1, "mainnet", "BTC", -3.0)
        rb.record_realized_pnl(1, "mainnet", "BTC", -2.0)
        rb.record_pnl_snapshot(1, "mainnet", "BTC", -1.5)
        # daily = realized(-5.0) + current_mtm(-1.5)
        assert rb.get_daily_pnl(1, "mainnet", "BTC") == pytest.approx(-6.5)

    def test_mtm_snapshot_overwrites_not_accumulates(self, store):
        rb.record_pnl_snapshot(1, "mainnet", "ETH", -2.0)
        rb.record_pnl_snapshot(1, "mainnet", "ETH", -5.0)
        rb.record_pnl_snapshot(1, "mainnet", "ETH", -1.0)
        # snapshot is the *current* mtm — last value wins, not the sum.
        assert rb.get_daily_pnl(1, "mainnet", "ETH") == pytest.approx(-1.0)

    def test_check_budget_ok(self, store):
        rb.record_pnl_snapshot(1, "mainnet", "BTC", -5.0)
        status = rb.check_budget(
            1, "mainnet", "BTC", soft_stop_usd=25.0, hard_stop_usd=50.0,
        )
        assert status["status"] == "ok"
        assert status["daily_pnl_usd"] == pytest.approx(-5.0)

    def test_check_budget_soft_stop(self, store):
        rb.record_realized_pnl(1, "mainnet", "BTC", -30.0)
        status = rb.check_budget(
            1, "mainnet", "BTC", soft_stop_usd=25.0, hard_stop_usd=50.0,
        )
        assert status["status"] == "soft_stopped"

    def test_check_budget_hard_stop_sets_cooldown(self, store):
        rb.record_realized_pnl(1, "mainnet", "BTC", -60.0)
        status = rb.check_budget(
            1, "mainnet", "BTC", soft_stop_usd=25.0, hard_stop_usd=50.0,
            cooldown_seconds=3600,
        )
        assert status["status"] == "hard_stopped"
        assert status["cooldown_remaining_s"] == pytest.approx(3600, abs=2)
        # Subsequent call — even with PnL recovered — still hard-stopped while
        # the cooldown window is active.
        rb.record_pnl_snapshot(1, "mainnet", "BTC", 100.0)  # huge recovery
        status2 = rb.check_budget(
            1, "mainnet", "BTC", soft_stop_usd=25.0, hard_stop_usd=50.0,
        )
        assert status2["status"] == "hard_stopped"
        assert "cooldown active" in status2["reason"]

    def test_clear_hard_stop(self, store):
        rb.record_realized_pnl(1, "mainnet", "BTC", -60.0)
        rb.check_budget(1, "mainnet", "BTC", soft_stop_usd=25.0, hard_stop_usd=50.0)
        rb.clear_hard_stop(1, "mainnet", "BTC")
        # PnL is still -60 so it will re-trip — but the *cooldown* was cleared.
        # To prove the clear worked, recover PnL first then check.
        rb.record_realized_pnl(1, "mainnet", "BTC", 70.0)  # net +10
        status = rb.check_budget(
            1, "mainnet", "BTC", soft_stop_usd=25.0, hard_stop_usd=50.0,
        )
        assert status["status"] == "ok"

    def test_zero_threshold_disables_rail(self, store):
        rb.record_realized_pnl(1, "mainnet", "BTC", -1000.0)
        status = rb.check_budget(
            1, "mainnet", "BTC", soft_stop_usd=0.0, hard_stop_usd=0.0,
        )
        # Both rails disabled (0) → never stops regardless of loss.
        assert status["status"] == "ok"

    def test_budget_is_per_product(self, store):
        rb.record_realized_pnl(1, "mainnet", "BTC", -60.0)
        # ETH budget untouched.
        eth_status = rb.check_budget(
            1, "mainnet", "ETH", soft_stop_usd=25.0, hard_stop_usd=50.0,
        )
        assert eth_status["status"] == "ok"


class TestResolveBudgetThresholds:
    def test_defaults_scale_with_notional(self):
        soft, hard, _ = rb.resolve_budget_thresholds({}, notional_usd=100.0)
        assert soft == pytest.approx(100.0)   # max(25, 100)
        assert hard == pytest.approx(200.0)   # max(50, 200)

    def test_small_notional_uses_floor(self):
        soft, hard, _ = rb.resolve_budget_thresholds({}, notional_usd=5.0)
        assert soft == pytest.approx(25.0)    # max(25, 5)
        assert hard == pytest.approx(50.0)    # max(50, 10)

    def test_explicit_state_overrides(self):
        soft, hard, cooldown = rb.resolve_budget_thresholds(
            {"risk_budget_soft_stop_usd": 10.0,
             "risk_budget_hard_stop_usd": 15.0,
             "risk_budget_hard_stop_cooldown_seconds": 600},
            notional_usd=100.0,
        )
        assert soft == pytest.approx(10.0)
        assert hard == pytest.approx(15.0)
        assert cooldown == 600


# ---------------------------------------------------------------------------
# Cross-strategy exposure registry
# ---------------------------------------------------------------------------

class TestExposureRegistry:
    def test_record_and_aggregate(self, store):
        rb.record_strategy_exposure(1, "mainnet", "BTC", "grid", 0.5, 5000.0)
        rb.record_strategy_exposure(1, "mainnet", "BTC", "vol", 0.2, 2000.0)
        agg = rb.get_product_exposure(1, "mainnet", "BTC")
        assert agg["net_units"] == pytest.approx(0.7)
        assert agg["gross_inv_usd"] == pytest.approx(7000.0)
        assert set(agg["by_strategy"].keys()) == {"grid", "vol"}
        assert agg["conflicting"] is False

    def test_conflicting_sides_detected(self, store):
        rb.record_strategy_exposure(1, "mainnet", "BTC", "grid", 0.5, 5000.0)
        rb.record_strategy_exposure(1, "mainnet", "BTC", "vol", -0.3, 3000.0)
        agg = rb.get_product_exposure(1, "mainnet", "BTC")
        assert agg["conflicting"] is True
        assert agg["net_units"] == pytest.approx(0.2)

    def test_stale_rows_excluded(self, store, monkeypatch):
        rb.record_strategy_exposure(1, "mainnet", "BTC", "grid", 0.5, 5000.0)
        # Hand-age the row past the stale window.
        key = rb._exposure_key(1, "mainnet", "BTC", "grid")
        row = store.get(key)
        row["updated_at"] = time.time() - rb.EXPOSURE_STALE_SECONDS - 60
        store.set(key, row)
        agg = rb.get_product_exposure(1, "mainnet", "BTC")
        assert agg["by_strategy"] == {}
        assert agg["net_units"] == pytest.approx(0.0)

    def test_flat_rows_excluded(self, store):
        rb.record_strategy_exposure(1, "mainnet", "BTC", "grid", 0.0, 0.0)
        agg = rb.get_product_exposure(1, "mainnet", "BTC")
        assert agg["by_strategy"] == {}

    def test_clear_strategy_exposure(self, store):
        rb.record_strategy_exposure(1, "mainnet", "BTC", "grid", 0.5, 5000.0)
        rb.clear_strategy_exposure(1, "mainnet", "BTC", "grid")
        agg = rb.get_product_exposure(1, "mainnet", "BTC")
        assert agg["by_strategy"] == {}


# ---------------------------------------------------------------------------
# mm_bot integration
# ---------------------------------------------------------------------------

class _FakeClient:
    def __init__(self, mid: float, positions=None):
        self.mid = mid
        self._positions = positions or []

    def get_market_price(self, _pid):
        return {"mid": self.mid, "best_bid": self.mid * 0.9999, "best_ask": self.mid * 1.0001}

    def get_all_positions(self):
        return list(self._positions)

    def get_open_orders(self, _pid):
        return []

    def get_balance(self):
        return {"exists": True, "balances": {0: 10_000.0, 2: 10_000.0},
                "available_balance": 10_000.0}

    def cancel_order(self, *_a, **_k):
        return {"success": True}

    def get_funding_rate(self, _pid):
        return {"funding_rate": 0.0}


class TestMmBotRiskBudgetIntegration:
    def _patch_store(self, monkeypatch):
        """Patch risk_budget's persistence as seen from mm_bot's import."""
        from src.nadobro.strategies import mm_bot
        s = _Store()
        monkeypatch.setattr(mm_bot._risk_budget, "get_bot_state", lambda k: s.get(k))
        monkeypatch.setattr(mm_bot._risk_budget, "set_bot_state", lambda k, v: s.set(k, v))
        monkeypatch.setattr(mm_bot._risk_budget, "execute", lambda *a, **k: None)
        return s, mm_bot

    def test_hard_stopped_product_blocks_quoting(self, monkeypatch):
        s, mm_bot = self._patch_store(monkeypatch)
        from src.nadobro.strategies import _regime
        monkeypatch.setattr(
            _regime, "classify_regime",
            lambda *a, **k: {
                "regime": _regime.REGIME_RANGE_WIDE, "confidence": 0.50,
                "realized_vol_bp": 15.0, "drift_bp": 2.0,
            },
        )
        # Pre-load a hard-stop cooldown for this product.
        key = mm_bot._risk_budget._budget_key(1, "mainnet", "BTC")
        s.set(key, {
            "realized_pnl_usd": -100.0, "current_mtm_usd": 0.0,
            "mtm_low_usd": -100.0,
            "hard_stop_until": time.time() + 3600, "updated_at": time.time(),
        })
        state = {
            "strategy": "dgrid", "product": "BTC", "spread_bp": 10.0,
            "levels": 2, "notional_usd": 100.0, "cycle_notional_usd": 100.0,
            "min_order_notional_usd": 20.0, "grid_stop_loss_pct": 100.0,
            "dgrid_intelligence_enabled": True,
        }
        placed: list = []
        monkeypatch.setattr(
            mm_bot, "execute_limit_order",
            lambda *a, **k: placed.append(1) or {"success": True, "digest": "d"},
        )
        result = mm_bot.run_cycle(
            telegram_id=1, network="mainnet", state=state,
            client=_FakeClient(mid=100.0, positions=[]), mid=100.0, open_orders=[],
        )
        assert result["success"] is True
        assert result.get("action") == "risk_budget_hard_stopped"
        assert placed == []
        assert "risk_budget" in result

    def test_budget_ok_proceeds(self, monkeypatch):
        s, mm_bot = self._patch_store(monkeypatch)
        from src.nadobro.strategies import _regime
        monkeypatch.setattr(
            _regime, "classify_regime",
            lambda *a, **k: {
                "regime": _regime.REGIME_RANGE_WIDE, "confidence": 0.50,
                "realized_vol_bp": 15.0, "drift_bp": 2.0,
            },
        )
        state = {
            "strategy": "dgrid", "product": "BTC", "spread_bp": 10.0,
            "levels": 1, "notional_usd": 100.0, "cycle_notional_usd": 100.0,
            "min_order_notional_usd": 20.0, "grid_stop_loss_pct": 100.0,
            "dgrid_intelligence_enabled": True,
        }
        placed: list = []
        monkeypatch.setattr(
            mm_bot, "execute_limit_order",
            lambda *a, **k: placed.append(1) or {"success": True, "digest": "d"},
        )
        result = mm_bot.run_cycle(
            telegram_id=1, network="mainnet", state=state,
            client=_FakeClient(mid=100.0, positions=[]), mid=100.0, open_orders=[],
        )
        assert result["success"] is True
        assert result.get("action") != "risk_budget_hard_stopped"
        assert result.get("action") != "risk_budget_soft_stopped"
        # Budget telemetry recorded, status ok.
        assert (state.get("mm_risk_budget") or {}).get("status") == "ok"

    def test_exposure_recorded_each_cycle(self, monkeypatch):
        s, mm_bot = self._patch_store(monkeypatch)
        from src.nadobro.strategies import _regime
        monkeypatch.setattr(
            _regime, "classify_regime",
            lambda *a, **k: {
                "regime": _regime.REGIME_RANGE_WIDE, "confidence": 0.50,
                "realized_vol_bp": 15.0, "drift_bp": 2.0,
            },
        )
        state = {
            "strategy": "dgrid", "product": "BTC", "spread_bp": 10.0,
            "levels": 1, "notional_usd": 100.0, "cycle_notional_usd": 100.0,
            "min_order_notional_usd": 20.0, "grid_stop_loss_pct": 100.0,
            "dgrid_intelligence_enabled": True,
        }
        monkeypatch.setattr(
            mm_bot, "execute_limit_order",
            lambda *a, **k: {"success": True, "digest": "d"},
        )
        # Long position so exposure is non-zero.
        positions = [{"product_id": 2, "amount": 0.1, "side": "LONG",
                      "unrealized_pnl": -1.0}]
        mm_bot.run_cycle(
            telegram_id=1, network="mainnet", state=state,
            client=_FakeClient(mid=100.0, positions=positions), mid=100.0,
            open_orders=[],
        )
        exp_key = mm_bot._risk_budget._exposure_key(1, "mainnet", "BTC", "dgrid")
        row = s.get(exp_key)
        assert row is not None
        assert row["net_units"] == pytest.approx(0.1)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
