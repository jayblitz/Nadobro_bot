"""Phase 4 — per-quote expectancy filter regression tests.

Locks in:

  1. Viable spread passes through unchanged.
  2. Tight spread auto-widens to the economically-viable level.
  3. Tight spread + auto-widen disabled → veto.
  4. Required spread above max_spread_bp → veto even with auto-widen.
  5. Per-strategy min-edge ordering (rgrid/mid demand thinner edge than dgrid).
  6. Chop regime demands a fatter post-cost cushion.
  7. Funding cost widens the required spread.
  8. mm_bot integration: economics veto returns quote_economics_skip.
  9. mm_bot integration: economics auto-widen lifts dynamic_spread_bp.
"""
from __future__ import annotations

import pytest

from src.nadobro.strategies import _quote_economics as qe
from src.nadobro.strategies import _regime


# ---------------------------------------------------------------------------
# Pure-function tests
# ---------------------------------------------------------------------------

class TestQuoteEconomicsPureFunction:
    def test_viable_spread_passes_through(self):
        # dgrid required spread = open(1.0) + close(2.6) + min_edge(4.0) = 7.6 bp
        econ = qe.evaluate_quote_economics(
            strategy="dgrid", spread_bp=10.0, regime=_regime.REGIME_RANGE_WIDE,
        )
        assert econ.viable is True
        assert econ.widened is False
        assert econ.recommended_spread_bp == pytest.approx(10.0)
        assert econ.required_spread_bp == pytest.approx(7.6, abs=0.01)
        assert econ.expected_edge_bp == pytest.approx(10.0 - 3.6, abs=0.01)

    def test_tight_spread_auto_widens(self):
        econ = qe.evaluate_quote_economics(
            strategy="dgrid", spread_bp=5.0, regime=_regime.REGIME_RANGE_WIDE,
            max_spread_bp=30.0,
        )
        assert econ.viable is True
        assert econ.widened is True
        # Widened up to the required spread (7.6 bp).
        assert econ.recommended_spread_bp == pytest.approx(7.6, abs=0.01)

    def test_tight_spread_auto_widen_disabled_vetoes(self):
        econ = qe.evaluate_quote_economics(
            strategy="dgrid", spread_bp=5.0, regime=_regime.REGIME_RANGE_WIDE,
            max_spread_bp=30.0, state={"quote_econ_auto_widen": False},
        )
        assert econ.viable is False
        assert econ.widened is False
        assert "auto-widen disabled" in econ.reason

    def test_required_above_max_spread_vetoes(self):
        # Required is 7.6 bp but max_spread caps at 6 bp → can't get viable.
        econ = qe.evaluate_quote_economics(
            strategy="dgrid", spread_bp=5.0, regime=_regime.REGIME_RANGE_WIDE,
            max_spread_bp=6.0,
        )
        assert econ.viable is False
        assert econ.widened is True
        assert econ.recommended_spread_bp == pytest.approx(6.0)
        assert "exceeds max_spread" in econ.reason

    def test_per_strategy_min_edge_ordering(self):
        # rgrid (2.0) and mid (1.5) demand thinner post-cost edge than dgrid (4.0).
        d = qe.evaluate_quote_economics(strategy="dgrid", spread_bp=10.0)
        r = qe.evaluate_quote_economics(strategy="rgrid", spread_bp=10.0)
        m = qe.evaluate_quote_economics(strategy="mid", spread_bp=10.0)
        assert d.min_required_edge_bp > r.min_required_edge_bp
        assert r.min_required_edge_bp > m.min_required_edge_bp
        # All three viable at 10 bp, but dgrid has the tightest margin.
        assert d.required_spread_bp > r.required_spread_bp > m.required_spread_bp

    def test_chop_regime_demands_fatter_cushion(self):
        wide = qe.evaluate_quote_economics(
            strategy="dgrid", spread_bp=10.0, regime=_regime.REGIME_RANGE_WIDE,
        )
        chop = qe.evaluate_quote_economics(
            strategy="dgrid", spread_bp=10.0, regime=_regime.REGIME_CHOP_HIGH_VOL,
        )
        # Chop multiplies min-edge by 2.0 → required spread is higher.
        assert chop.required_spread_bp > wide.required_spread_bp
        assert chop.min_required_edge_bp == pytest.approx(8.0, abs=0.01)

    def test_funding_cost_widens_required_spread(self):
        no_funding = qe.evaluate_quote_economics(strategy="dgrid", spread_bp=10.0)
        with_funding = qe.evaluate_quote_economics(
            strategy="dgrid", spread_bp=10.0, funding_bp_per_hour=3.0,
        )
        assert with_funding.funding_cost_bp == pytest.approx(3.0, abs=0.01)
        assert with_funding.required_spread_bp > no_funding.required_spread_bp
        assert with_funding.total_cost_bp == pytest.approx(
            no_funding.total_cost_bp + 3.0, abs=0.01
        )

    def test_state_override_min_edge(self):
        econ = qe.evaluate_quote_economics(
            strategy="dgrid", spread_bp=10.0,
            state={"quote_econ_min_edge_bp": 1.0},
        )
        assert econ.min_required_edge_bp == pytest.approx(1.0)

    def test_to_dict_round_trips(self):
        econ = qe.evaluate_quote_economics(strategy="dgrid", spread_bp=10.0)
        d = econ.to_dict()
        assert d["viable"] is True
        assert "expected_edge_bp" in d
        assert "required_spread_bp" in d


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


class TestMmBotEconomicsIntegration:
    def test_economics_veto_when_max_spread_too_low(self, monkeypatch):
        # range_wide regime so the Phase 3 gate doesn't fire — we want the
        # Phase 4 economics check to be the thing that vetoes.
        from src.nadobro.strategies import mm_bot, _regime
        monkeypatch.setattr(
            _regime, "classify_regime",
            lambda *a, **k: {
                "regime": _regime.REGIME_RANGE_WIDE, "confidence": 0.50,
                "realized_vol_bp": 15.0, "drift_bp": 2.0,
            },
        )
        # spread 3 bp, max_spread 5 bp → required ~7.6 bp can't be reached.
        state = {
            "strategy": "dgrid",
            "product": "BTC",
            "spread_bp": 3.0,
            "max_spread_bp": 5.0,
            "levels": 2,
            "notional_usd": 100.0,
            "cycle_notional_usd": 100.0,
            "min_order_notional_usd": 20.0,
            "grid_stop_loss_pct": 100.0,
            "dgrid_intelligence_enabled": True,
        }
        placed: list = []
        monkeypatch.setattr(
            mm_bot, "execute_limit_order",
            lambda *a, **k: placed.append(1) or {"success": True, "digest": "d"},
        )
        client = _FakeClient(mid=100.0, positions=[])
        result = mm_bot.run_cycle(
            telegram_id=1, network="mainnet", state=state,
            client=client, mid=100.0, open_orders=[],
        )
        assert result["success"] is True
        assert result.get("action") == "quote_economics_skip"
        assert placed == []
        assert "quote_economics" in result

    def test_economics_auto_widen_lifts_spread(self, monkeypatch):
        from src.nadobro.strategies import mm_bot, _regime
        monkeypatch.setattr(
            _regime, "classify_regime",
            lambda *a, **k: {
                "regime": _regime.REGIME_RANGE_WIDE, "confidence": 0.50,
                "realized_vol_bp": 15.0, "drift_bp": 2.0,
            },
        )
        # spread 3 bp, max_spread 30 bp → auto-widens to ~7.6 bp and quotes.
        state = {
            "strategy": "dgrid",
            "product": "BTC",
            "spread_bp": 3.0,
            "max_spread_bp": 30.0,
            "levels": 1,
            "notional_usd": 100.0,
            "cycle_notional_usd": 100.0,
            "min_order_notional_usd": 20.0,
            "grid_stop_loss_pct": 100.0,
            "dgrid_intelligence_enabled": True,
        }
        placed: list = []
        monkeypatch.setattr(
            mm_bot, "execute_limit_order",
            lambda *a, **k: placed.append(1) or {"success": True, "digest": "d"},
        )
        client = _FakeClient(mid=100.0, positions=[])
        result = mm_bot.run_cycle(
            telegram_id=1, network="mainnet", state=state,
            client=client, mid=100.0, open_orders=[],
        )
        assert result["success"] is True
        assert result.get("action") != "quote_economics_skip"
        econ = state.get("mm_quote_economics") or {}
        assert econ.get("widened") is True
        # Orders were placed at the widened spread.
        assert placed, "expected orders placed after auto-widen"

    def test_economics_off_when_intelligence_disabled(self, monkeypatch):
        from src.nadobro.strategies import mm_bot, _regime
        monkeypatch.setattr(
            _regime, "classify_regime",
            lambda *a, **k: {
                "regime": _regime.REGIME_RANGE_WIDE, "confidence": 0.50,
                "realized_vol_bp": 15.0, "drift_bp": 2.0,
            },
        )
        state = {
            "strategy": "dgrid",
            "product": "BTC",
            "spread_bp": 3.0,
            "max_spread_bp": 5.0,
            "levels": 1,
            "notional_usd": 100.0,
            "cycle_notional_usd": 100.0,
            "min_order_notional_usd": 20.0,
            "grid_stop_loss_pct": 100.0,
            "dgrid_intelligence_enabled": False,
        }
        placed: list = []
        monkeypatch.setattr(
            mm_bot, "execute_limit_order",
            lambda *a, **k: placed.append(1) or {"success": True, "digest": "d"},
        )
        client = _FakeClient(mid=100.0, positions=[])
        result = mm_bot.run_cycle(
            telegram_id=1, network="mainnet", state=state,
            client=client, mid=100.0, open_orders=[],
        )
        assert result["success"] is True
        # Economics filter is a no-op when intelligence is off.
        assert result.get("action") != "quote_economics_skip"
        assert placed, "orders should place with intelligence off"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
