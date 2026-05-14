"""Phase 3 — regime-gated quoting regression tests.

Locks in:

  1. Tight range + high confidence  → skip both sides (early-exit).
  2. CHOP_HIGH_VOL                  → cap levels + widen spread.
  3. CHOP_HIGH_VOL very high conf   → skip both sides.
  4. TREND_DOWN + high conf         → skip BUY rungs.
  5. TREND_UP + high conf           → skip SELL rungs.
  6. R-Grid reversed mode in trend  → gate permits both sides (don't fight
                                      the mode switcher).
  7. Volume Bot tight-range veto    → entry blocked when regime is tight.
  8. Below confidence threshold     → gate is a no-op.
"""
from __future__ import annotations

import sys
import types

import pytest

from src.nadobro.strategies import _quote_gate as qg
from src.nadobro.strategies import _regime
from src.nadobro.strategies import volume_bot as vol


# ---------------------------------------------------------------------------
# Pure-function tests on evaluate_quote_gate
# ---------------------------------------------------------------------------

class TestQuoteGatePureFunction:
    def test_tight_range_high_conf_skips_both(self):
        d = qg.evaluate_quote_gate(
            strategy="dgrid",
            regime_info={"regime": _regime.REGIME_RANGE_TIGHT, "confidence": 0.90},
        )
        assert d.skip_buy and d.skip_sell
        assert d.active
        assert "range_tight" in d.reason

    def test_tight_range_low_conf_no_op(self):
        d = qg.evaluate_quote_gate(
            strategy="dgrid",
            regime_info={"regime": _regime.REGIME_RANGE_TIGHT, "confidence": 0.40},
        )
        assert not d.active
        assert not d.skip_buy and not d.skip_sell

    def test_chop_caps_levels_and_widens_spread(self):
        d = qg.evaluate_quote_gate(
            strategy="dgrid",
            regime_info={"regime": _regime.REGIME_CHOP_HIGH_VOL, "confidence": 0.70},
        )
        assert d.active
        assert not d.skip_buy and not d.skip_sell
        assert d.level_cap == 1
        assert d.spread_widen_mult > 1.0

    def test_chop_very_high_conf_skips_both(self):
        d = qg.evaluate_quote_gate(
            strategy="dgrid",
            regime_info={"regime": _regime.REGIME_CHOP_HIGH_VOL, "confidence": 0.92},
        )
        assert d.skip_buy and d.skip_sell
        assert d.active

    def test_trend_down_skips_buy_only(self):
        d = qg.evaluate_quote_gate(
            strategy="dgrid",
            regime_info={"regime": _regime.REGIME_TREND_DOWN, "confidence": 0.85},
        )
        assert d.active
        assert d.skip_buy
        assert not d.skip_sell

    def test_trend_up_skips_sell_only(self):
        d = qg.evaluate_quote_gate(
            strategy="dgrid",
            regime_info={"regime": _regime.REGIME_TREND_UP, "confidence": 0.85},
        )
        assert d.active
        assert d.skip_sell
        assert not d.skip_buy

    def test_rgrid_reversed_mode_permits_trend_side(self):
        # R-Grid in REVERSED mode is intentionally trend-following — the
        # gate should not fight it. classic mode → skip adverse side
        # (same as D-Grid). reversed mode → no skip.
        classic_state = {"rgrid_active_mode": "classic"}
        reversed_state = {"rgrid_active_mode": "reversed"}

        d_classic = qg.evaluate_quote_gate(
            strategy="rgrid",
            regime_info={"regime": _regime.REGIME_TREND_DOWN, "confidence": 0.90},
            state=classic_state,
        )
        d_reversed = qg.evaluate_quote_gate(
            strategy="rgrid",
            regime_info={"regime": _regime.REGIME_TREND_DOWN, "confidence": 0.90},
            state=reversed_state,
        )
        assert d_classic.skip_buy
        assert not d_reversed.skip_buy
        assert not d_reversed.skip_sell

    def test_mid_more_eager_to_skip_than_dgrid(self):
        # Mid Mode runs tightest spreads, so the gate config drops it out
        # of tight ranges at a lower confidence than D-Grid.
        regime = {"regime": _regime.REGIME_RANGE_TIGHT, "confidence": 0.58}
        d_dgrid = qg.evaluate_quote_gate(strategy="dgrid", regime_info=regime)
        d_mid = qg.evaluate_quote_gate(strategy="mid", regime_info=regime)
        assert not d_dgrid.active  # D-Grid threshold 0.65
        assert d_mid.active        # Mid threshold 0.55

    def test_state_overrides_strategy_default(self):
        d = qg.evaluate_quote_gate(
            strategy="dgrid",
            regime_info={"regime": _regime.REGIME_RANGE_TIGHT, "confidence": 0.50},
            state={"quote_gate_skip_tight_min_confidence": 0.40},
        )
        # 0.50 confidence is above the user override 0.40 → veto fires.
        assert d.active
        assert d.skip_buy and d.skip_sell

    def test_no_regime_info_returns_inactive(self):
        d = qg.evaluate_quote_gate(strategy="dgrid", regime_info=None)
        assert not d.active

    def test_rgrid_loses_in_chop_skips_both(self):
        # R-Grid has the most aggressive chop_skip_both_min_confidence (0.70)
        # because reversed-mode flips have no follow-through.
        d = qg.evaluate_quote_gate(
            strategy="rgrid",
            regime_info={"regime": _regime.REGIME_CHOP_HIGH_VOL, "confidence": 0.78},
        )
        assert d.skip_buy and d.skip_sell


# ---------------------------------------------------------------------------
# apply_gate_to_orders helper
# ---------------------------------------------------------------------------

class TestApplyGateToOrders:
    def _orders(self):
        return [
            {"price": 99.0, "is_long": True,  "level": 1},
            {"price": 98.5, "is_long": True,  "level": 2},
            {"price": 101.0, "is_long": False, "level": 1},
            {"price": 101.5, "is_long": False, "level": 2},
        ]

    def test_skip_buy_drops_only_buys(self):
        d = qg.QuoteGateDecision(skip_buy=True, active=True)
        out = qg.apply_gate_to_orders(d, self._orders())
        assert all(not o["is_long"] for o in out)
        assert len(out) == 2

    def test_skip_sell_drops_only_sells(self):
        d = qg.QuoteGateDecision(skip_sell=True, active=True)
        out = qg.apply_gate_to_orders(d, self._orders())
        assert all(o["is_long"] for o in out)

    def test_level_cap_drops_deeper_levels(self):
        d = qg.QuoteGateDecision(level_cap=1, active=True)
        out = qg.apply_gate_to_orders(d, self._orders())
        assert len(out) == 2
        assert all(o["level"] == 1 for o in out)

    def test_inactive_pass_through(self):
        d = qg.QuoteGateDecision(active=False)
        out = qg.apply_gate_to_orders(d, self._orders())
        assert len(out) == 4


# ---------------------------------------------------------------------------
# mm_bot integration: gate vetoes the whole cycle
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


class TestMmBotGateIntegration:
    def test_tight_range_skips_quote_placement(self, monkeypatch):
        # Force a high-confidence tight-range regime by stubbing the classifier.
        from src.nadobro.strategies import mm_bot, _regime
        monkeypatch.setattr(
            _regime, "classify_regime",
            lambda *a, **k: {
                "regime": _regime.REGIME_RANGE_TIGHT, "confidence": 0.95,
                "realized_vol_bp": 1.5, "drift_bp": 0.0,
            },
        )
        state = {
            "strategy": "dgrid",
            "product": "BTC",
            "spread_bp": 10.0,
            "levels": 2,
            "notional_usd": 100.0,
            "cycle_notional_usd": 100.0,
            "min_order_notional_usd": 20.0,
            "grid_stop_loss_pct": 100.0,
            "dgrid_intelligence_enabled": True,
        }
        placed: list = []

        def _record_order(*args, **kwargs):
            placed.append({"args": args, "kwargs": kwargs})
            return {"success": True, "digest": f"d{len(placed)}"}

        monkeypatch.setattr(mm_bot, "execute_limit_order", _record_order)
        client = _FakeClient(mid=100.0, positions=[])
        result = mm_bot.run_cycle(
            telegram_id=1, network="mainnet", state=state,
            client=client, mid=100.0, open_orders=[],
        )
        assert result["success"] is True
        # The gate veto path returns early with action quote_gate_veto.
        assert result.get("action") == "quote_gate_veto"
        # No new orders should have been placed.
        assert placed == []
        # Gate decision should be exposed for telemetry.
        assert "quote_gate" in result

    def test_trend_down_drops_buy_rungs(self, monkeypatch):
        from src.nadobro.strategies import mm_bot, _regime
        monkeypatch.setattr(
            _regime, "classify_regime",
            lambda *a, **k: {
                "regime": _regime.REGIME_TREND_DOWN, "confidence": 0.85,
                "realized_vol_bp": 20.0, "drift_bp": -25.0,
            },
        )
        state = {
            "strategy": "dgrid",
            "product": "BTC",
            "spread_bp": 10.0,
            "levels": 2,
            "notional_usd": 100.0,
            "cycle_notional_usd": 100.0,
            "min_order_notional_usd": 20.0,
            "grid_stop_loss_pct": 100.0,
            "dgrid_intelligence_enabled": True,
        }
        sides: list[bool] = []

        def _record_order(*args, **kwargs):
            # is_long is positional arg index 4 in execute_limit_order calls
            # made from mm_bot — but the cleaner check is the kwargs version.
            is_long = kwargs.get("is_long")
            if is_long is None and len(args) >= 5:
                is_long = bool(args[4])
            sides.append(bool(is_long))
            return {"success": True, "digest": f"d{len(sides)}"}

        monkeypatch.setattr(mm_bot, "execute_limit_order", _record_order)
        client = _FakeClient(mid=100.0, positions=[])
        result = mm_bot.run_cycle(
            telegram_id=1, network="mainnet", state=state,
            client=client, mid=100.0, open_orders=[],
        )
        assert result["success"] is True
        # In trend_down the gate drops BUYs (is_long=True). Only SELLs (False) remain.
        assert sides, "expected at least one sell order placed"
        assert not any(sides), f"expected no buy orders, got is_long flags {sides}"

    def test_gate_off_when_intelligence_disabled(self, monkeypatch):
        from src.nadobro.strategies import mm_bot, _regime
        # Even though the regime would veto, intelligence is disabled, so
        # the gate must be a no-op. Stubbing classify_regime to avoid the
        # warm-up branch returning low confidence.
        monkeypatch.setattr(
            _regime, "classify_regime",
            lambda *a, **k: {
                "regime": _regime.REGIME_RANGE_TIGHT, "confidence": 0.95,
                "realized_vol_bp": 1.5, "drift_bp": 0.0,
            },
        )
        state = {
            "strategy": "dgrid",
            "product": "BTC",
            "spread_bp": 10.0,
            "levels": 1,
            "notional_usd": 100.0,
            "cycle_notional_usd": 100.0,
            "min_order_notional_usd": 20.0,
            "grid_stop_loss_pct": 100.0,
            "dgrid_intelligence_enabled": False,
        }
        placed: list = []

        def _record_order(*args, **kwargs):
            placed.append(1)
            return {"success": True, "digest": "d"}

        monkeypatch.setattr(mm_bot, "execute_limit_order", _record_order)
        client = _FakeClient(mid=100.0, positions=[])
        result = mm_bot.run_cycle(
            telegram_id=1, network="mainnet", state=state,
            client=client, mid=100.0, open_orders=[],
        )
        assert result["success"] is True
        # Quote gate is a no-op when intelligence is off — orders proceed.
        assert result.get("action") != "quote_gate_veto"
        assert placed, "orders should have been placed with intelligence off"


# ---------------------------------------------------------------------------
# Volume Bot tight-range veto
# ---------------------------------------------------------------------------

class TestVolumeBotTightVeto:
    def test_tight_range_blocks_entry(self, monkeypatch):
        signal = {"ok": True, "direction": "long", "edge_bp": 12.0}
        # Pretend intelligence is enabled and tight-range regime is firing.
        monkeypatch.setattr(vol, "_vol_intelligence_on", lambda _s: True)
        monkeypatch.setattr(
            vol._vol_regime, "classify_regime",
            lambda *a, **k: {
                "regime": vol._vol_regime.REGIME_RANGE_TIGHT,
                "confidence": 0.85,
                "drift_bp": 0.0,
            },
        )
        out_signal, mult = vol._vol_regime_gate(
            signal=signal, state={}, history=[], mid=100.0, product="BTC",
        )
        assert out_signal["ok"] is False
        assert out_signal["reason"] == "regime_range_tight_no_edge"
        assert mult == 1.0

    def test_tight_range_low_conf_passes_through(self, monkeypatch):
        signal = {"ok": True, "direction": "long", "edge_bp": 12.0}
        monkeypatch.setattr(vol, "_vol_intelligence_on", lambda _s: True)
        monkeypatch.setattr(
            vol._vol_regime, "classify_regime",
            lambda *a, **k: {
                "regime": vol._vol_regime.REGIME_RANGE_TIGHT,
                "confidence": 0.45,
                "drift_bp": 0.0,
            },
        )
        out_signal, _ = vol._vol_regime_gate(
            signal=signal, state={}, history=[], mid=100.0, product="BTC",
        )
        assert out_signal["ok"] is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
