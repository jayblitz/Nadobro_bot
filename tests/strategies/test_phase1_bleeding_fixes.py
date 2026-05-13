"""Phase 1 — "stop the bleeding" regression tests.

These lock in the four mechanical fixes from the strategic audit:

  1. Grid soft-reset no longer dumps inventory below the entry VWAP.
  2. R-Grid reference price no longer drags with one-sided fills.
  3. Volume Bot entry gates tighter (RSI 40/60, min_edge_bp = 12).
  4. PM trail arm scales with margin (not a flat $0.50 floor).

If any of these regress, the bot resumes the "buy low, sell below entry"
pattern that produced the 13 May WTI -$13.12 day.
"""
from __future__ import annotations

import sys
import types
import unittest
from unittest.mock import patch

from src.nadobro.strategies import _position_manager as pm
from src.nadobro.strategies import mm_bot
from src.nadobro.strategies import volume_bot as vol


# ---------------------------------------------------------------------------
# 1. Grid soft-reset: never sell below entry VWAP + min exit edge.
# ---------------------------------------------------------------------------

class GridSoftResetExitFloorTests(unittest.TestCase):
    def test_soft_reset_sell_clamps_above_buy_vwap_plus_edge(self):
        # Long inventory bought at VWAP $100. Price dropped to best_ask $98.
        # Legacy behaviour: replace sell rungs with best_ask = $98 → guaranteed
        # 2% loss per closing fill. New behaviour: clamp sell ≥ 100 * (1 + 3bp).
        quotes = mm_bot._compute_grid_prices(
            reference_price=99.5,
            spread_bp=10.0,
            levels=2,
            strategy="grid",
            mid_price=98.0,
            best_bid=97.95,
            best_ask=98.0,
            soft_reset_side="sell",
            exit_floor_buy_vwap=100.0,
            min_exit_edge_bp=3.0,
        )
        sells = [q["price"] for q in quotes if not q["is_long"]]
        self.assertTrue(sells)
        for s in sells:
            self.assertGreaterEqual(s, 100.0 * (1 + 3 / 10000.0) - 1e-9,
                                    f"sell {s} below entry-vwap floor")

    def test_soft_reset_buy_clamps_below_sell_vwap_minus_edge(self):
        # Short inventory sold at VWAP $100. Price rallied to best_bid $102.
        # Buys (to flatten) must stay below $100 - edge so we never pay above
        # the original short entry.
        quotes = mm_bot._compute_grid_prices(
            reference_price=100.5,
            spread_bp=10.0,
            levels=2,
            strategy="grid",
            mid_price=102.0,
            best_bid=102.0,
            best_ask=102.05,
            soft_reset_side="buy",
            exit_floor_sell_vwap=100.0,
            min_exit_edge_bp=3.0,
        )
        buys = [q["price"] for q in quotes if q["is_long"]]
        self.assertTrue(buys)
        for b in buys:
            self.assertLessEqual(b, 100.0 * (1 - 3 / 10000.0) + 1e-9,
                                 f"buy {b} above entry-vwap ceiling")

    def test_soft_reset_without_floor_still_works_for_backcompat(self):
        # When the caller doesn't pass exit_floor_*, the legacy chase-the-market
        # behaviour kicks in (no clamp). Confirms the new parameter is optional.
        quotes = mm_bot._compute_grid_prices(
            reference_price=99.5,
            spread_bp=10.0,
            levels=1,
            strategy="grid",
            mid_price=98.0,
            best_bid=97.95,
            best_ask=98.0,
            soft_reset_side="sell",
            # exit_floor_buy_vwap left as default (None)
        )
        sells = [q["price"] for q in quotes if not q["is_long"]]
        # Without a floor we fall back to best_ask.
        self.assertAlmostEqual(sells[0], 98.0, places=6)


# ---------------------------------------------------------------------------
# 2. R-Grid anchor: don't drag with one-sided fills.
# ---------------------------------------------------------------------------

class _FakeClient:
    """Minimal client surface used by mm_bot.run_cycle."""
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
        return {"exists": True, "balances": {0: 10_000.0, 2: 10_000.0}, "available_balance": 10_000.0}

    def cancel_order(self, *_args, **_kwargs):
        return {"success": True}

    def get_funding_rate(self, _pid):
        return {"funding_rate": 0.0}


class RGridAnchorTests(unittest.TestCase):
    def test_rgrid_one_sided_buy_fills_do_not_drag_reference(self):
        """One-sided buy fills in a downtrend used to set
        ``reference_price = grid_anchor_price = buy_vwap``, so subsequent sell
        rungs ended up below the original entries. The ema_mid anchor mode
        must keep reference_price near mid when only one side has filled.
        """
        # Seed buy_fills only (one-sided downtrend filling our buys).
        state = {
            "strategy": "rgrid",
            "product": "BTC",
            "rgrid_spread_bp": 10.0,
            "levels": 1,
            "notional_usd": 50.0,
            "cycle_notional_usd": 50.0,
            "min_order_notional_usd": 20.0,
            "rgrid_stop_loss_pct": 100.0,
            "grid_buy_fills": [
                {"price": 100.5, "size": 0.05, "ts": 1.0},
                {"price": 100.2, "size": 0.05, "ts": 2.0},
                {"price": 99.9, "size": 0.05, "ts": 3.0},
            ],
            "grid_sell_fills": [],
            "grid_anchor_price": 100.5,
        }
        client = _FakeClient(mid=99.5, positions=[])

        def _ok_order(*_a, **_k):
            return {"success": True, "digest": "d"}

        with patch.object(mm_bot, "execute_limit_order", side_effect=_ok_order):
            result = mm_bot.run_cycle(
                telegram_id=1, network="mainnet", state=state,
                client=client, mid=99.5, open_orders=[],
            )

        self.assertTrue(result["success"])
        # Old behaviour: reference_price would track the buy VWAP (~100.2),
        # not the mid (99.5). With ema_mid mode, reference_price should be
        # close to mid (not dragged down toward buy VWAP).
        ref = float(result.get("reference_price") or 0.0)
        # Allow small EMA lag — but the anchor must NOT be the buy VWAP.
        self.assertGreater(ref, 99.5 - 0.30,
                           f"reference_price {ref} drifted too far below mid")
        self.assertLess(ref, 100.4,
                        f"reference_price {ref} dragged toward buy-VWAP "
                        f"(would be ~100.2 under legacy fill_vwap mode)")

    def test_rgrid_legacy_fill_vwap_mode_preserved(self):
        """anchor_mode='fill_vwap' restores the old reference = grid_anchor
        behaviour for users who explicitly want it."""
        state = {
            "strategy": "rgrid",
            "product": "BTC",
            "rgrid_spread_bp": 10.0,
            "levels": 1,
            "notional_usd": 50.0,
            "cycle_notional_usd": 50.0,
            "min_order_notional_usd": 20.0,
            "rgrid_stop_loss_pct": 100.0,
            "anchor_mode": "fill_vwap",
            "grid_buy_fills": [{"price": 100.0, "size": 0.10, "ts": 1.0}],
            "grid_sell_fills": [],
        }
        client = _FakeClient(mid=99.5, positions=[])

        with patch.object(mm_bot, "execute_limit_order",
                          side_effect=lambda *a, **k: {"success": True, "digest": "d"}):
            result = mm_bot.run_cycle(
                telegram_id=1, network="mainnet", state=state,
                client=client, mid=99.5, open_orders=[],
            )

        self.assertTrue(result["success"])
        # In legacy mode the reference_price snaps to the buy VWAP (~100.0).
        ref = float(result.get("reference_price") or 0.0)
        self.assertAlmostEqual(ref, 100.0, delta=0.05)


# ---------------------------------------------------------------------------
# 3. Volume Bot entry gates retuned.
# ---------------------------------------------------------------------------

class VolumeBotDefaultsTests(unittest.TestCase):
    def test_rsi_thresholds_tightened(self):
        self.assertEqual(vol.DEFAULT_VOL_RSI_LONG_MAX, 40.0)
        self.assertEqual(vol.DEFAULT_VOL_RSI_SHORT_MIN, 60.0)
        # No-trade band must exist (long_max < short_min).
        self.assertLess(vol.DEFAULT_VOL_RSI_LONG_MAX, vol.DEFAULT_VOL_RSI_SHORT_MIN)

    def test_min_edge_bp_above_round_trip_cost(self):
        # Round-trip cost on Tread is ~5 bp (maker + slippage). Entry edge
        # must be greater than that or the strategy has negative expectancy.
        self.assertGreaterEqual(vol.DEFAULT_VOL_MIN_EDGE_BP, 10.0)


# ---------------------------------------------------------------------------
# 4. PM trail arm scales with margin.
# ---------------------------------------------------------------------------

class PmTrailArmPctTests(unittest.TestCase):
    def test_trail_arm_pct_default_present(self):
        cfg = pm._pm_config({"strategy": "dgrid"})
        self.assertIn("trail_arm_pct", cfg)
        self.assertGreater(cfg["trail_arm_pct"], 0.0)
        # Explicit USD setting absent → falls through to pct-of-margin.
        self.assertFalse(cfg["trail_arm_usd_explicit"])

    def test_explicit_trail_arm_usd_wins(self):
        cfg = pm._pm_config({"strategy": "dgrid", "pm_trail_arm_usd": 2.50})
        self.assertTrue(cfg["trail_arm_usd_explicit"])
        self.assertAlmostEqual(cfg["trail_arm_usd"], 2.50)

    def test_pct_arm_scales_with_position_size(self):
        cfg = pm._pm_config({"strategy": "dgrid"})  # pct = 0.60, USD floor = 0.50

        # Small position: $108 notional × 0.60% = $0.648 arm.
        small_arm = pm._resolve_trail_arm_usd(cfg, inv_u=1.0, vwap=108.0, leverage=1.0)
        # Large position: $10,000 notional × 0.60% = $60 arm.
        large_arm = pm._resolve_trail_arm_usd(cfg, inv_u=100.0, vwap=100.0, leverage=1.0)
        self.assertGreater(large_arm, small_arm * 50)

    def test_pct_arm_falls_back_to_usd_floor_when_tiny(self):
        cfg = pm._pm_config({"strategy": "dgrid"})
        # Cheap product: $0.10 notional × 0.60% = $0.0006 — way below USD floor.
        tiny_arm = pm._resolve_trail_arm_usd(cfg, inv_u=1.0, vwap=0.10, leverage=1.0)
        # Should clamp to the USD floor instead.
        self.assertGreaterEqual(tiny_arm, cfg["trail_arm_usd"])


# ---------------------------------------------------------------------------
# 5. Bro Mode TP/SL ratios fixed.
# ---------------------------------------------------------------------------

class BroModeTpSlTests(unittest.TestCase):
    def test_bro_fallback_tp_sl_have_better_reward_risk(self):
        from src.nadobro.strategies import bro_mode

        snapshot = {
            "assets": [
                {
                    "product": "BTC",
                    "signal_1h": "bullish",
                    "ema_9": 110.0, "ema_21": 105.0, "ema_50": 100.0,
                    "change_1h": 1.0, "change_4h": 2.0,
                    "rsi_14": 55.0,
                    "funding_rate": -0.0001,
                    "regime": "trending_up",
                }
            ],
        }
        decision = bro_mode._fallback_bro_decision(
            snapshot, positions=[], min_confidence=0.4, max_leverage=5,
        )
        self.assertIn(decision["action"], ("open_long", "open_short"))
        rr = decision["tp_pct"] / decision["sl_pct"]
        self.assertGreaterEqual(rr, 2.0, f"reward:risk {rr:.2f}x too thin")


# ---------------------------------------------------------------------------
# 6. Delta Neutral defaults to 'wait'.
# ---------------------------------------------------------------------------

class DnFundingDefaultTests(unittest.TestCase):
    def test_default_funding_mode_is_wait(self):
        # Read the source-level default: the run_cycle function resolves
        # state.get("funding_entry_mode") or "wait", so when no key is set
        # the bot doesn't blindly enter at unfavorable funding.
        import inspect

        from src.nadobro.strategies import delta_neutral

        source = inspect.getsource(delta_neutral.run_cycle)
        self.assertIn('"wait"', source)
        self.assertNotIn('or "enter_anyway"', source,
                         "default funding_entry_mode should be 'wait', not 'enter_anyway'")


if __name__ == "__main__":
    unittest.main()
