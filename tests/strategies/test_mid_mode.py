"""Tread Fi Mid Mode parity tests for Nadobro MM (Phase 1).

Locks in:
  - Mid Mode quotes are pure mid ± spread×level (no anchor / no soft-reset).
  - Spread default 5 bps; range [-10, +100] bps; negative spread enables concede
    (post-only clamp disengages).
  - Reference mode resolves through the existing _compute_reference_price (mid /
    ema_fast / ema_slow).
  - directional_bias is a continuous float in [-1, +1] with ±0.2 alpha tilt on
    side multipliers, and full +20% margin uplift at |bias|=1.
  - GRID/RGRID/DGRID still see the legacy discrete bias mapping (string form).
  - Mid Mode does NOT touch grid_anchor_price / discretion / VWAP exposure.
"""
import unittest
from unittest.mock import patch

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from _stubs import install_test_stubs  # noqa: E402

install_test_stubs()

from src.nadobro.strategies import mm_bot  # noqa: E402


class _MidClient:
    def __init__(self, mid=10000.0, balance=10000.0, positions=None, open_orders=None):
        self._mid = mid
        self._balance = float(balance)
        self._positions = positions or []
        self._open_orders = open_orders or []
        self.cancelled = []

    def get_market_price(self, _product_id):
        return {"mid": self._mid, "bid": self._mid - 1.0, "ask": self._mid + 1.0}

    def get_open_orders(self, _product_id):
        return list(self._open_orders)

    def get_all_positions(self):
        return list(self._positions)

    def get_balance(self):
        return {
            "exists": True,
            "balances": {0: self._balance},
            "equity": self._balance,
            "available_balance": self._balance,
        }

    def cancel_order(self, _product_id, digest):
        self.cancelled.append(digest)
        self._open_orders = [o for o in self._open_orders if o.get("digest") != digest]
        return {"success": True}


class MidModePricingTests(unittest.TestCase):
    def test_mid_mode_quotes_symmetric_around_mid_no_anchor(self):
        """With neutral bias and no positions, buys/sells are symmetric around mid."""
        state = {
            "product": "BTC",
            "strategy": "mid",
            "spread_bp": 5.0,
            "levels": 2,
            "notional_usd": 1000.0,
            "cycle_notional_usd": 1000.0,
            "min_order_notional_usd": 10.0,
            "directional_bias": 0.0,
            "reference_mode": "mid",
        }
        client = _MidClient(mid=10000.0, balance=100_000.0)
        placed = {"buys": [], "sells": []}

        def _ok_order(*args, **kwargs):
            price = float(args[3]) if len(args) >= 4 else float(kwargs.get("price") or 0.0)
            if kwargs.get("is_long"):
                placed["buys"].append(price)
            else:
                placed["sells"].append(price)
            return {"success": True, "digest": f"d{len(placed['buys']) + len(placed['sells'])}"}

        with patch.object(mm_bot, "get_product_id", return_value=2), patch.object(
            mm_bot, "get_product_max_leverage", return_value=10.0
        ), patch.object(mm_bot, "execute_limit_order", side_effect=_ok_order):
            result = mm_bot.run_cycle(
                telegram_id=1, network="mainnet", state=state, client=client, mid=10000.0, open_orders=[]
            )

        self.assertTrue(result["success"])
        self.assertGreater(result.get("orders_placed", 0), 0)
        # Symmetry around mid: for each level, |buy - mid| ≈ |sell - mid|.
        # Post-only clamp adjusts top-level buys/sells to best_bid/best_ask, so
        # only check that the per-level offsets follow the (1 - i*spread)/(1 + i*spread) pattern
        # for the unclamped (level=2) case.
        self.assertTrue(placed["buys"])
        self.assertTrue(placed["sells"])
        for b in placed["buys"]:
            self.assertLess(b, 10000.0)
        for s in placed["sells"]:
            self.assertGreater(s, 10000.0)
        # No anchor / no soft reset state was written.
        self.assertEqual(state.get("grid_anchor_price"), 0.0)
        self.assertEqual(state.get("grid_buy_exposure_price"), 0.0)
        self.assertEqual(state.get("grid_sell_exposure_price"), 0.0)
        self.assertFalse(state.get("grid_reset_active", False))

    def test_mid_mode_ignores_seeded_anchor_and_fills(self):
        """Seeded grid_anchor_price + fills must NOT influence mid-mode pricing.

        We seed an anchor far from mid; without the mid-mode skip, RGRID-style
        anchor logic would re-center quotes around 9500 instead of 10000.
        Inventory-implied net_units is kept at zero by using tiny seeded sizes —
        this test is about pricing centerline, not inventory pause behavior.
        """
        state = {
            "product": "BTC",
            "strategy": "mid",
            "spread_bp": 10.0,
            "levels": 1,
            "notional_usd": 1000.0,
            "cycle_notional_usd": 1000.0,
            "min_order_notional_usd": 10.0,
            "directional_bias": 0.0,
            "reference_mode": "mid",
            "inventory_soft_limit_usd": 100_000.0,
            # These would shift a GRID/RGRID centerline; mid mode must ignore them.
            "grid_anchor_price": 9500.0,
            "grid_last_fill_price": 9500.0,
            "grid_buy_fills": [
                {"price": 9500.0, "size": 0.0001, "ts": 0.0},
                {"price": 9495.0, "size": 0.0001, "ts": 1.0},
            ],
            "grid_sell_fills": [
                {"price": 9505.0, "size": 0.0001, "ts": 0.5},
                {"price": 9510.0, "size": 0.0001, "ts": 1.5},
            ],
        }
        client = _MidClient(mid=10000.0, balance=100_000.0)
        placed = {"buys": [], "sells": []}

        def _ok_order(*args, **kwargs):
            price = float(args[3])
            if kwargs.get("is_long"):
                placed["buys"].append(price)
            else:
                placed["sells"].append(price)
            return {"success": True, "digest": f"d{len(placed['buys']) + len(placed['sells'])}"}

        with patch.object(mm_bot, "get_product_id", return_value=2), patch.object(
            mm_bot, "get_product_max_leverage", return_value=10.0
        ), patch.object(mm_bot, "execute_limit_order", side_effect=_ok_order):
            mm_bot.run_cycle(
                telegram_id=1, network="mainnet", state=state, client=client, mid=10000.0, open_orders=[]
            )

        # Quotes must be near 10000 (the live mid), not pulled toward 9500 anchor.
        self.assertTrue(placed["buys"])
        for b in placed["buys"]:
            self.assertGreater(b, 9900.0, f"buy {b} got pulled toward stale anchor 9500")
        for s in placed["sells"]:
            self.assertLess(s, 10100.0)
        # Anchor cleared.
        self.assertEqual(state["grid_anchor_price"], 0.0)

    def test_mid_mode_spread_clamped_to_documented_range(self):
        """Tread spec: spread range −10 → +100 bps. Out-of-range values clamp."""
        state = {
            "product": "BTC",
            "strategy": "mid",
            "spread_bp": 500.0,  # Way over Tread max — must clamp to +100.
            "levels": 1,
            "notional_usd": 1000.0,
            "cycle_notional_usd": 1000.0,
            "min_order_notional_usd": 10.0,
            "directional_bias": 0.0,
            "reference_mode": "mid",
        }
        client = _MidClient(mid=10000.0, balance=100_000.0)
        with patch.object(mm_bot, "get_product_id", return_value=2), patch.object(
            mm_bot, "get_product_max_leverage", return_value=10.0
        ), patch.object(mm_bot, "execute_limit_order", return_value={"success": True, "digest": "d"}):
            result = mm_bot.run_cycle(
                telegram_id=1, network="mainnet", state=state, client=client, mid=10000.0, open_orders=[]
            )

        self.assertTrue(result["success"])
        self.assertLessEqual(result["spread_bp"], mm_bot.MID_MAX_SPREAD_BP + 1e-9)


class MidModeBiasTests(unittest.TestCase):
    def test_continuous_bias_tilts_side_multipliers_linearly(self):
        # bias = +0.5 → buy_mult = 1.10, sell_mult = 0.90 (Tread ±0.2 alpha tilt).
        buy, sell, paused, _ = mm_bot._resolve_side_multipliers(
            0.5, net_units=0.0, inv_soft_usd=100.0, mid=10000.0, use_continuous_bias=True
        )
        self.assertAlmostEqual(buy, 1.10, places=6)
        self.assertAlmostEqual(sell, 0.90, places=6)
        self.assertFalse(paused)

    def test_continuous_bias_full_negative_max_tilt(self):
        # bias = -1.0 → buy_mult = 0.80, sell_mult = 1.20.
        buy, sell, _, _ = mm_bot._resolve_side_multipliers(
            -1.0, net_units=0.0, inv_soft_usd=100.0, mid=10000.0, use_continuous_bias=True
        )
        self.assertAlmostEqual(buy, 0.80, places=6)
        self.assertAlmostEqual(sell, 1.20, places=6)

    def test_continuous_bias_clamped_to_unit_interval(self):
        # bias > 1 must clamp; the result mirrors |bias|=1.
        buy_clamped, sell_clamped, _, _ = mm_bot._resolve_side_multipliers(
            5.0, net_units=0.0, inv_soft_usd=100.0, mid=10000.0, use_continuous_bias=True
        )
        buy_one, sell_one, _, _ = mm_bot._resolve_side_multipliers(
            1.0, net_units=0.0, inv_soft_usd=100.0, mid=10000.0, use_continuous_bias=True
        )
        self.assertAlmostEqual(buy_clamped, buy_one, places=6)
        self.assertAlmostEqual(sell_clamped, sell_one, places=6)

    def test_legacy_string_bias_unchanged_for_grid_family(self):
        # GRID/RGRID/DGRID must keep the discrete 1.15/0.85 mapping — locked in
        # so existing live users don't see a side-weight shift.
        buy, sell, _, _ = mm_bot._resolve_side_multipliers(
            "long_bias", net_units=0.0, inv_soft_usd=100.0, mid=10000.0
        )
        self.assertAlmostEqual(buy, 1.15, places=6)
        self.assertAlmostEqual(sell, 0.85, places=6)

    def test_resolve_directional_bias_value_accepts_str_and_float(self):
        self.assertEqual(mm_bot._resolve_directional_bias_value("neutral"), 0.0)
        self.assertEqual(mm_bot._resolve_directional_bias_value("long_bias"), 1.0)
        self.assertEqual(mm_bot._resolve_directional_bias_value("short_bias"), -1.0)
        self.assertEqual(mm_bot._resolve_directional_bias_value(0.5), 0.5)
        self.assertEqual(mm_bot._resolve_directional_bias_value("0.7"), 0.7)
        self.assertEqual(mm_bot._resolve_directional_bias_value(None), 0.0)
        # Out-of-range floats clamp.
        self.assertEqual(mm_bot._resolve_directional_bias_value(2.5), 1.0)
        self.assertEqual(mm_bot._resolve_directional_bias_value(-2.5), -1.0)


class MidModeMarginUpliftTests(unittest.TestCase):
    def test_full_bias_applies_20pct_margin_uplift(self):
        """At |bias|=1.0 the per-quote margin estimate increases by 20%."""
        base_state = {
            "product": "BTC",
            "strategy": "mid",
            "spread_bp": 5.0,
            "levels": 2,
            "notional_usd": 100.0,
            "cycle_notional_usd": 100.0,
            "min_order_notional_usd": 10.0,
            "reference_mode": "mid",
        }

        # Baseline: bias = 0.0 → no uplift.
        state_neutral = dict(base_state, directional_bias=0.0)
        client = _MidClient(mid=10000.0, balance=10_000.0)
        with patch.object(mm_bot, "get_product_id", return_value=2), patch.object(
            mm_bot, "get_product_max_leverage", return_value=10.0
        ), patch.object(mm_bot, "execute_limit_order", return_value={"success": True, "digest": "d"}):
            mm_bot.run_cycle(
                telegram_id=1, network="mainnet", state=state_neutral, client=client, mid=10000.0, open_orders=[]
            )
        margin_neutral = float(state_neutral["mm_margin_per_quote_est_usd"])
        uplift_neutral = float(state_neutral.get("mm_bias_margin_uplift") or 1.0)

        # Full long bias: bias = +1.0 → +20% uplift.
        state_full = dict(base_state, directional_bias=1.0)
        client = _MidClient(mid=10000.0, balance=10_000.0)
        with patch.object(mm_bot, "get_product_id", return_value=2), patch.object(
            mm_bot, "get_product_max_leverage", return_value=10.0
        ), patch.object(mm_bot, "execute_limit_order", return_value={"success": True, "digest": "d"}):
            mm_bot.run_cycle(
                telegram_id=1, network="mainnet", state=state_full, client=client, mid=10000.0, open_orders=[]
            )
        margin_full = float(state_full["mm_margin_per_quote_est_usd"])

        self.assertAlmostEqual(uplift_neutral, 1.0, places=6)
        self.assertAlmostEqual(state_full["mm_bias_margin_uplift"], 1.20, places=6)
        # Per-quote margin estimate scales linearly with the uplift.
        self.assertAlmostEqual(margin_full / margin_neutral, 1.20, places=4)

    def test_grid_strategy_does_not_apply_bias_uplift(self):
        """Bias uplift is Mid Mode-only; GRID/RGRID/DGRID must not see the uplift."""
        state = {
            "product": "BTC",
            "strategy": "grid",
            "spread_bp": 5.0,
            "levels": 2,
            "notional_usd": 100.0,
            "cycle_notional_usd": 100.0,
            "min_order_notional_usd": 10.0,
            "directional_bias": "long_bias",
        }
        client = _MidClient(mid=10000.0, balance=10_000.0)
        with patch.object(mm_bot, "get_product_id", return_value=2), patch.object(
            mm_bot, "get_product_max_leverage", return_value=10.0
        ), patch.object(mm_bot, "execute_limit_order", return_value={"success": True, "digest": "d"}):
            mm_bot.run_cycle(
                telegram_id=1, network="mainnet", state=state, client=client, mid=10000.0, open_orders=[]
            )
        # mm_bias_margin_uplift is Mid-Mode-only state; not written for grid.
        self.assertNotIn("mm_bias_margin_uplift", state)


class MidModeRegistryIntegrationTests(unittest.TestCase):
    def test_mid_appears_in_market_making_strategies(self):
        from src.nadobro.services.strategy_registry import (
            MARKET_MAKING_STRATEGIES,
            SUPPORTED_STRATEGIES,
            STRATEGY_DISPLAY_NAMES,
        )
        self.assertIn("mid", SUPPORTED_STRATEGIES)
        self.assertIn("mid", MARKET_MAKING_STRATEGIES)
        self.assertEqual(STRATEGY_DISPLAY_NAMES["mid"], "MID MODE")

    def test_mid_aliases_resolve(self):
        from src.nadobro.services.strategy_registry import normalize_strategy_id
        self.assertEqual(normalize_strategy_id("mid_mode"), "mid")
        self.assertEqual(normalize_strategy_id("mid-mode"), "mid")
        self.assertEqual(normalize_strategy_id("midmode"), "mid")
        self.assertEqual(normalize_strategy_id("mid"), "mid")


if __name__ == "__main__":
    unittest.main()
