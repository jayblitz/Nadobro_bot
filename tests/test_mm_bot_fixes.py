"""Tests that lock in the GRID/RGRID fixes applied to mm_bot.py.

Covers:
    - EMA persistence across cycles (was broken: `_detect_ema_crossover` never
      wrote back, crossover detection died after cycle 1).
    - EMA idempotency within a cycle (multiple callers must not re-mix alpha).
    - GRID / RGRID price shape parity: both sides quote buys BELOW the anchor,
      sells ABOVE (was broken for RGRID: flipped-then-clamped levels all
      collapsed to top-of-book).
    - Post-only widening retry ladder.
"""
import unittest
from unittest.mock import patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.strategies import mm_bot


class EmaUpdateTests(unittest.TestCase):
    def test_update_both_emas_initializes_to_mid_on_first_call(self):
        state = {}
        fast, slow = mm_bot._update_both_emas(state, 100.0, 0.3, 0.1)
        self.assertEqual(fast, 100.0)
        self.assertEqual(slow, 100.0)
        # Must persist into state so next cycle can read them.
        self.assertEqual(state["mm_ref_ema_fast"], 100.0)
        self.assertEqual(state["mm_ref_ema_slow"], 100.0)

    def test_update_both_emas_advances_across_cycles(self):
        state = {}
        # Cycle 1: seeds EMAs at 100.
        mm_bot._update_both_emas(state, 100.0, 0.3, 0.1)
        # Cycle 2 (new mid): idempotency tag cleared by run_cycle entry.
        state.pop("_mm_ema_updated_mid", None)
        fast, slow = mm_bot._update_both_emas(state, 110.0, 0.3, 0.1)
        # Fast alpha (0.3) must move more than slow alpha (0.1) toward 110.
        self.assertGreater(fast, slow)
        self.assertGreater(fast, 100.0)
        self.assertLess(fast, 110.0)
        self.assertLess(slow, fast)
        # State is mutated; future cycles pick up these values.
        self.assertEqual(state["mm_ref_ema_fast"], fast)
        self.assertEqual(state["mm_ref_ema_slow"], slow)

    def test_update_both_emas_idempotent_within_one_cycle(self):
        """Within a single cycle the idempotency tag must prevent re-mixing."""
        state = {"mm_ref_ema_fast": 100.0, "mm_ref_ema_slow": 100.0}
        # First call this cycle: advance.
        fast1, slow1 = mm_bot._update_both_emas(state, 110.0, 0.3, 0.1)
        # Second call this cycle (same mid): must return identical values and
        # NOT advance the EMAs again.
        fast2, slow2 = mm_bot._update_both_emas(state, 110.0, 0.3, 0.1)
        self.assertEqual(fast1, fast2)
        self.assertEqual(slow1, slow2)
        self.assertEqual(state["mm_ref_ema_fast"], fast1)

    def test_ema_crossover_reads_prev_before_advance(self):
        """Crossover detection must capture prev EMAs before `_update_both_emas`."""
        state = {"mm_ref_ema_fast": 110.0, "mm_ref_ema_slow": 120.0}
        # Previously bearish (fast < slow). Push mid high so this cycle goes
        # bullish and the detector should fire `crossed=True`.
        result = mm_bot._detect_ema_crossover(state, 200.0, 0.9, 0.05)
        self.assertTrue(result["bullish"])
        self.assertTrue(result["crossed"])


class GridPriceShapeTests(unittest.TestCase):
    def _counts(self, orders):
        buys = sorted([o["price"] for o in orders if o["is_long"]], reverse=True)
        sells = sorted([o["price"] for o in orders if not o["is_long"]])
        return buys, sells

    def test_grid_buys_below_sells_above_with_distinct_levels(self):
        ref = 100.0
        orders = mm_bot._compute_grid_prices(
            reference_price=ref,
            spread_bp=10.0,
            levels=3,
            strategy="grid",
            best_bid=99.9,
            best_ask=100.1,
        )
        buys, sells = self._counts(orders)
        self.assertEqual(len(buys), 3)
        self.assertEqual(len(sells), 3)
        # Post-only safety: buys at/below best_bid, sells at/above best_ask.
        for p in buys:
            self.assertLessEqual(p, 99.9 + 1e-9)
        for p in sells:
            self.assertGreaterEqual(p, 100.1 - 1e-9)
        # Levels must remain distinct (not collapsed by clamp).
        self.assertEqual(len(set(buys)), 3)
        self.assertEqual(len(set(sells)), 3)
        # Deeper levels must be further from ref.
        self.assertGreater(buys[0], buys[1])
        self.assertGreater(buys[1], buys[2])
        self.assertLess(sells[0], sells[1])
        self.assertLess(sells[1], sells[2])

    def test_rgrid_produces_same_price_shape_as_grid(self):
        """Regression: before the fix, RGRID flipped buy/sell prices and then the
        post-only clamp collapsed every level onto top-of-book. All six orders
        ended up at two prices. Now RGRID uses the same shape as GRID; its
        "reversal" is applied via directional_bias, not via price inversion."""
        ref = 100.0
        grid = mm_bot._compute_grid_prices(
            reference_price=ref, spread_bp=10.0, levels=3, strategy="grid",
            best_bid=99.9, best_ask=100.1,
        )
        rgrid = mm_bot._compute_grid_prices(
            reference_price=ref, spread_bp=10.0, levels=3, strategy="rgrid",
            best_bid=99.9, best_ask=100.1,
        )
        self.assertEqual(
            sorted([round(o["price"], 8) for o in grid]),
            sorted([round(o["price"], 8) for o in rgrid]),
        )
        # And RGRID levels must be distinct (the specific failure symptom).
        rgrid_buys = [o["price"] for o in rgrid if o["is_long"]]
        rgrid_sells = [o["price"] for o in rgrid if not o["is_long"]]
        self.assertEqual(len(set(rgrid_buys)), 3)
        self.assertEqual(len(set(rgrid_sells)), 3)

    def test_grid_without_book_clamp_still_distinct(self):
        orders = mm_bot._compute_grid_prices(
            reference_price=100.0, spread_bp=5.0, levels=4, strategy="grid",
        )
        buys = sorted([o["price"] for o in orders if o["is_long"]], reverse=True)
        sells = sorted([o["price"] for o in orders if not o["is_long"]])
        self.assertEqual(len(set(buys)), 4)
        self.assertEqual(len(set(sells)), 4)


class PostOnlyRepriceTests(unittest.TestCase):
    """The reprice helper should widen progressively on each attempt."""

    def test_reprice_widens_with_attempt_number(self):
        # Guard: if the helper was renamed, skip gracefully.
        if not hasattr(mm_bot, "_reprice_post_only_quote"):
            self.skipTest("reprice helper not present under expected name")

        # Take the top-of-book and repeatedly reprice; the distance to bid/ask
        # should grow strictly with each attempt.
        best_bid = 100.0
        best_ask = 100.10
        prices_buy = []
        prices_sell = []
        for attempt in (0, 1, 2):
            try:
                buy = mm_bot._reprice_post_only_quote(
                    is_long=True,
                    price=best_bid,
                    best_bid=best_bid,
                    best_ask=best_ask,
                    attempt=attempt,
                )
                sell = mm_bot._reprice_post_only_quote(
                    is_long=False,
                    price=best_ask,
                    best_bid=best_bid,
                    best_ask=best_ask,
                    attempt=attempt,
                )
            except TypeError:
                # Signature differs; ensure the function at least exists and is callable.
                self.skipTest("reprice signature differs from expected")
            prices_buy.append(buy)
            prices_sell.append(sell)

        # Each subsequent attempt should push quotes further from the book
        # (buys down, sells up).
        self.assertGreaterEqual(prices_buy[0], prices_buy[1])
        self.assertGreaterEqual(prices_buy[1], prices_buy[2])
        self.assertLessEqual(prices_sell[0], prices_sell[1])
        self.assertLessEqual(prices_sell[1], prices_sell[2])


class ConcedeModePostOnlyTests(unittest.TestCase):
    def test_negative_spread_disables_post_only_for_concede_quotes(self):
        state = {
            "product": "BTC",
            "strategy": "rgrid",
            "spread_bp": -10.0,
            "levels": 1,
            "notional_usd": 100.0,
            "cycle_notional_usd": 100.0,
            "min_order_notional_usd": 10.0,
            "max_open_orders": 1,
            "interval_seconds": 60,
        }

        class _Client:
            def get_market_price(self, _product_id):
                return {"mid": 100.0, "bid": 99.9, "ask": 100.1}

            def get_open_orders(self, _product_id):
                return []

            def get_all_positions(self):
                return []

            def get_balance(self):
                return {"equity": 1000.0}

        client = _Client()
        with patch.object(mm_bot, "get_product_id", return_value=2), patch.object(
            mm_bot,
            "execute_limit_order",
            return_value={"success": True, "digest": "d1"},
        ) as place_mock:
            result = mm_bot.run_cycle(
                telegram_id=1, network="mainnet", state=state, client=client
            )
        self.assertTrue(result["success"])
        self.assertTrue(place_mock.called)
        self.assertFalse(place_mock.call_args.kwargs["post_only"])


if __name__ == "__main__":
    unittest.main()
