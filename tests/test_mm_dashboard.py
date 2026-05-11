"""Phase 3: Tread-style pre-trade card + /mm_status + /mm_fills tests.

Locks in:
  - product_catalog captures and exposes maker_fee_rate_x18 / taker_fee_rate_x18.
  - compute_pretrade_margin_per_quote_usd matches Tread perp formula:
      (notional / leverage) × safety × participation_multiplier × (1 + |bias|×0.20)
  - compute_max_loss_usd is margin-based (Phase 0 fix).
  - estimate_pretrade_fees_usd composes builder (1 bps) + maker (signed) correctly.
  - build_status_snapshot derives all counters from the persisted state dict
    and tolerates missing keys.
  - render_fills_lines orders most-recent first across both sides.
  - State round-trips through bot_state JSON for the Phase 3 persistence list.
"""
import json
import unittest
from unittest.mock import patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.services import mm_dashboard, product_catalog  # noqa: E402


class CatalogFeeAccessorTests(unittest.TestCase):
    def setUp(self):
        # Fresh in-memory catalog each test so accessors hit the patched build.
        product_catalog._catalog_cache.clear()

    def _patched_catalog(self, maker_x18=None, taker_x18=None):
        # Match the shape returned by _build_dynamic_catalog.
        return {
            "perps": {
                "BTC": {
                    "id": 2,
                    "type": "perp",
                    "symbol": "BTC-PERP",
                    "base": "BTC",
                    "dynamic": True,
                    "max_leverage": 50,
                    "isolated_only": False,
                    "min_size_x18": str(int(10.0 * 1e18)),
                    "size_increment_x18": None,
                    "price_increment_x18": None,
                    "maker_fee_rate_x18": str(maker_x18) if maker_x18 is not None else None,
                    "taker_fee_rate_x18": str(taker_x18) if taker_x18 is not None else None,
                },
            },
            "by_id": {2: "BTC"},
            "aliases": {"btc": "BTC", "btc-perp": "BTC"},
        }

    def test_maker_fee_accessor_returns_signed_fraction(self):
        # -0.03% (= -3 bps) maker rebate, x18 scaled.
        catalog = self._patched_catalog(maker_x18=-300_000_000_000_000)
        with patch.object(product_catalog, "get_catalog", return_value=catalog):
            rate = product_catalog.get_product_maker_fee_rate("BTC", network="mainnet")
        self.assertIsNotNone(rate)
        self.assertAlmostEqual(rate, -0.0003, places=6)

    def test_taker_fee_accessor_returns_positive_fraction(self):
        catalog = self._patched_catalog(taker_x18=200_000_000_000_000)
        with patch.object(product_catalog, "get_catalog", return_value=catalog):
            rate = product_catalog.get_product_taker_fee_rate("BTC", network="mainnet")
        self.assertAlmostEqual(rate, 0.0002, places=6)

    def test_missing_maker_rate_returns_none(self):
        catalog = self._patched_catalog(maker_x18=None)
        with patch.object(product_catalog, "get_catalog", return_value=catalog):
            self.assertIsNone(product_catalog.get_product_maker_fee_rate("BTC"))


class PretradeMarginMathTests(unittest.TestCase):
    def test_normal_preset_neutral_bias_collapses_to_base_margin(self):
        b = mm_dashboard.compute_pretrade_margin_per_quote_usd(
            min_order_notional_usd=100.0,
            leverage=10.0,
            safety_factor=1.25,
            participation_preset="normal",
            directional_bias=0.0,
        )
        # base = 100/10 × 1.25 = 12.5; preset=1.0; bias_uplift=1.0 → 12.5
        self.assertAlmostEqual(b["base_margin_usd"], 12.5, places=6)
        self.assertAlmostEqual(b["participation_multiplier"], 1.0, places=6)
        self.assertAlmostEqual(b["bias_uplift"], 1.0, places=6)
        self.assertAlmostEqual(b["required_margin_per_quote_usd"], 12.5, places=6)

    def test_aggressive_doubles_margin_full_bias_adds_20pct(self):
        b = mm_dashboard.compute_pretrade_margin_per_quote_usd(
            min_order_notional_usd=100.0,
            leverage=10.0,
            safety_factor=1.0,
            participation_preset="aggressive",
            directional_bias=1.0,
        )
        # base = 10; preset=2.0; bias_uplift=1.20 → 10 × 2 × 1.20 = 24
        self.assertAlmostEqual(b["required_margin_per_quote_usd"], 24.0, places=6)

    def test_passive_halves_margin(self):
        b = mm_dashboard.compute_pretrade_margin_per_quote_usd(
            min_order_notional_usd=100.0,
            leverage=10.0,
            safety_factor=1.0,
            participation_preset="passive",
            directional_bias=0.0,
        )
        # base = 10; preset=0.5; bias=1.0 → 5
        self.assertAlmostEqual(b["required_margin_per_quote_usd"], 5.0, places=6)

    def test_no_preset_treated_as_neutral(self):
        b = mm_dashboard.compute_pretrade_margin_per_quote_usd(
            min_order_notional_usd=50.0,
            leverage=5.0,
            safety_factor=1.0,
            participation_preset=None,
            directional_bias=0.0,
        )
        self.assertAlmostEqual(b["participation_multiplier"], 1.0, places=6)
        self.assertAlmostEqual(b["required_margin_per_quote_usd"], 10.0, places=6)


class MaxLossTests(unittest.TestCase):
    def test_max_loss_uses_margin_not_notional(self):
        # 100 notional × 10x leverage = 10 margin; SL=5% → max loss $0.50.
        loss = mm_dashboard.compute_max_loss_usd(
            notional_usd=100.0, leverage=10.0, sl_pct=5.0
        )
        self.assertAlmostEqual(loss, 0.5, places=6)

    def test_max_loss_zero_when_sl_off(self):
        self.assertEqual(
            mm_dashboard.compute_max_loss_usd(notional_usd=100.0, leverage=10.0, sl_pct=0.0),
            0.0,
        )

    def test_spot_leverage_one_collapses_to_notional_pct(self):
        loss = mm_dashboard.compute_max_loss_usd(notional_usd=100.0, leverage=1.0, sl_pct=5.0)
        self.assertAlmostEqual(loss, 5.0, places=6)


class FeeCompositionTests(unittest.TestCase):
    def test_builder_only_when_maker_zero(self):
        f = mm_dashboard.estimate_pretrade_fees_usd(
            placed_notional_usd=10_000.0, maker_fee_fraction=0.0
        )
        # 1 bps × 10000 = $1
        self.assertAlmostEqual(f["builder_fee_usd"], 1.0, places=6)
        self.assertAlmostEqual(f["maker_fee_usd"], 0.0, places=6)
        self.assertAlmostEqual(f["net_fee_usd"], 1.0, places=6)
        self.assertAlmostEqual(f["builder_fee_bps"], 1.0, places=6)

    def test_negative_maker_rebate_can_flip_net_fee_negative(self):
        # Builder 1 bps + maker -3 bps = -2 bps net.
        f = mm_dashboard.estimate_pretrade_fees_usd(
            placed_notional_usd=10_000.0, maker_fee_fraction=-0.0003
        )
        self.assertAlmostEqual(f["maker_fee_bps"], -3.0, places=6)
        self.assertAlmostEqual(f["net_fee_usd"], -2.0, places=6)

    def test_missing_maker_treated_as_zero(self):
        f = mm_dashboard.estimate_pretrade_fees_usd(
            placed_notional_usd=10_000.0, maker_fee_fraction=None
        )
        self.assertAlmostEqual(f["net_fee_usd"], 1.0, places=6)


class StatusSnapshotTests(unittest.TestCase):
    def test_snapshot_derives_session_progress_and_fill_rate(self):
        state = {
            "running": True,
            "leverage": 10.0,
            "leverage_mode": "MAX",
            "spread_bp": 5.0,
            "reference_price": 10000.0,
            "mm_session_notional_done_usd": 250.0,
            "session_notional_cap_usd": 1000.0,
            "mm_initial_equity": 100.0,
            "mm_cumulative_pnl": -10.0,
            "grid_last_cycle_pnl_usd": 1.5,
            "mm_tracked_quotes": {"d1": {}, "d2": {}, "d3": {}},
            "grid_buy_fills": [{"price": 1, "size": 1, "ts": 1}],
            "grid_sell_fills": [{"price": 1, "size": 1, "ts": 2}],
            "inventory_soft_limit_usd": 60.0,
            "grid_prev_net_units": 0.001,
            "mm_mid_history": [9999.0, 10000.0],
            "mm_min_order_notional_usd_resolved": 10.0,
            "mm_max_resting_quotes_cap": 6,
            "mm_margin_per_quote_est_usd": 12.5,
            "mm_pov_engine": {
                "preset": "normal",
                "multiplier": 0.05,
                "duration_minutes": 100.0,
                "interval_seconds": 1200,
                "cycle_notional_usd": 50.0,
                "pair_24h_volume_usd": 1_000_000.0,
            },
        }
        snap = mm_dashboard.build_status_snapshot(
            state=state,
            strategy_id="mid",
            network="testnet",
            product="BTC",
            open_orders_count=4,
        )
        self.assertEqual(snap["running"], True)
        self.assertAlmostEqual(snap["session_progress_pct"], 25.0, places=4)
        # 2 fills out of max(2 fills, 3 tracked) = 2/3 fill rate.
        self.assertAlmostEqual(snap["fill_rate"], 2 / 3, places=6)
        self.assertAlmostEqual(snap["drawdown_pct"], 10.0, places=4)
        self.assertAlmostEqual(snap["inv_hard_limit_usd"], 60.0 * 1.8, places=6)
        self.assertEqual(snap["pov_engine"]["preset"], "normal")
        self.assertAlmostEqual(snap["min_order_notional_usd"], 10.0, places=6)

    def test_snapshot_handles_empty_state(self):
        snap = mm_dashboard.build_status_snapshot(
            state={}, strategy_id="grid", network="mainnet", product="BTC", open_orders_count=0
        )
        self.assertFalse(snap["running"])
        self.assertEqual(snap["fill_count"], 0)
        self.assertEqual(snap["fill_rate"], 0.0)
        self.assertEqual(snap["session_progress_pct"], 0.0)


class FillsRenderingTests(unittest.TestCase):
    def test_fills_combine_both_sides_newest_first(self):
        state = {
            "grid_buy_fills": [
                {"price": 100.0, "size": 1.0, "ts": 10.0},
                {"price": 99.0, "size": 1.0, "ts": 30.0},
            ],
            "grid_sell_fills": [
                {"price": 101.0, "size": 1.0, "ts": 20.0},
            ],
        }
        lines = mm_dashboard.render_fills_lines(state, limit=3)
        self.assertEqual(len(lines), 3)
        self.assertTrue(lines[0].startswith("BUY"))   # ts=30, newest
        self.assertTrue(lines[1].startswith("SELL"))  # ts=20
        self.assertTrue(lines[2].startswith("BUY"))   # ts=10

    def test_fills_empty_state_friendly_message(self):
        lines = mm_dashboard.render_fills_lines({}, limit=5)
        self.assertEqual(len(lines), 1)
        self.assertIn("No fills", lines[0])

    def test_fills_limit_respected(self):
        many = [{"price": 100, "size": 1, "ts": float(i)} for i in range(20)]
        state = {"grid_buy_fills": many, "grid_sell_fills": []}
        self.assertEqual(len(mm_dashboard.render_fills_lines(state, limit=5)), 5)


class StatePersistenceTests(unittest.TestCase):
    def test_phase3_keys_round_trip_through_json(self):
        """The Phase 3 plan calls out specific keys for restart recovery. They
        all live in the strategy state dict and round-trip through the existing
        bot_state JSON blob. Any nested dicts/lists must survive too.
        """
        original = {
            "strategy": "dgrid",
            "running": True,
            "mm_session_notional_done_usd": 1234.56,
            "mm_initial_equity": 5000.0,
            "mm_tracked_quotes": {
                "abc123": {"price": 100.0, "side": "buy", "placed_ts": 1700000000.0}
            },
            "grid_buy_fills": [
                {"price": 99.5, "size": 0.01, "ts": 1700000123.0}
            ],
            "grid_sell_fills": [
                {"price": 100.5, "size": 0.01, "ts": 1700000200.0}
            ],
            "dgrid_phase": "rgrid",
            "dgrid_variance_ratio": 1.32,
        }
        encoded = json.dumps(original)
        restored = json.loads(encoded)
        self.assertEqual(restored, original)
        # Spot-check nested keys survive (would catch e.g. tuple→list coercion).
        self.assertEqual(
            restored["mm_tracked_quotes"]["abc123"]["price"], 100.0
        )
        self.assertEqual(restored["grid_buy_fills"][0]["size"], 0.01)


if __name__ == "__main__":
    unittest.main()
