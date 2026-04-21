import unittest
from unittest.mock import patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.strategies import mm_bot


class _FakeClient:
    def __init__(self, mid=100.0, positions=None, open_orders=None):
        self._mid = mid
        self._positions = positions or []
        self._open_orders = open_orders or []
        self.cancelled = []

    def get_market_price(self, _product_id):
        return {"mid": self._mid}

    def get_all_positions(self):
        return list(self._positions)

    def get_open_orders(self, _product_id):
        return list(self._open_orders)

    def cancel_order(self, _product_id, digest):
        self.cancelled.append(digest)
        self._open_orders = [o for o in self._open_orders if o.get("digest") != digest]
        return {"success": True}


class MmProfitabilityTests(unittest.TestCase):
    def test_reference_mode_and_volatility_adjust_spread(self):
        state = {
            "strategy": "grid",
            "product": "BTC",
            "spread_bp": 4.0,
            "levels": 2,
            "reference_mode": "ema_fast",
            "ema_fast_alpha": 0.5,
            "mm_ref_ema_fast": 100.0,
            "vol_window_points": 6,
            "vol_sensitivity": 0.2,
            "min_spread_bp": 2.0,
            "max_spread_bp": 30.0,
            "notional_usd": 100.0,
            "cycle_notional_usd": 100.0,
            "min_order_notional_usd": 20.0,
            "mm_mid_history": [100, 110, 90, 112, 88],
        }
        client = _FakeClient(mid=105.0)
        placed = []

        def _ok_order(*_args, **kwargs):
            placed.append(kwargs.get("price"))
            idx = len(placed)
            return {"success": True, "digest": f"d{idx}"}

        with patch.object(mm_bot, "execute_limit_order", side_effect=_ok_order):
            result = mm_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client, mid=105.0, open_orders=[])

        self.assertTrue(result["success"])
        self.assertGreater(result["spread_bp"], 4.0)
        self.assertNotEqual(round(result["reference_price"], 8), 105.0)
        self.assertGreater(result["orders_placed"], 0)
        self.assertIn("mm_last_metrics", state)

    def test_hard_inventory_limit_flattens_only_one_side(self):
        state = {
            "strategy": "grid",
            "product": "BTC",
            "spread_bp": 4.0,
            "levels": 2,
            "notional_usd": 100.0,
            "cycle_notional_usd": 100.0,
            "min_order_notional_usd": 20.0,
            "inventory_soft_limit_usd": 20.0,
        }
        # Long inventory ($50) breaches hard limit (~$36+), so only sell quotes should be placed.
        client = _FakeClient(
            mid=100.0,
            positions=[{"product_id": 2, "amount": 0.5, "side": "LONG"}],
        )
        sides = []

        def _ok_order(*_args, **kwargs):
            sides.append(bool(kwargs.get("is_long")))
            return {"success": True, "digest": f"d{len(sides)}"}

        with patch.object(mm_bot, "execute_limit_order", side_effect=_ok_order):
            result = mm_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client, mid=100.0, open_orders=[])

        self.assertTrue(result["success"])
        self.assertTrue(state.get("mm_paused"))
        self.assertTrue(state.get("mm_pause_reason"))
        self.assertTrue(sides)  # At least one order was attempted
        self.assertTrue(all(side is False for side in sides))  # False = sell side only

    def test_twap_cycle_budget_respects_session_cap(self):
        state = {
            "strategy": "grid",
            "product": "BTC",
            "spread_bp": 4.0,
            "levels": 2,
            "notional_usd": 100.0,
            "cycle_notional_usd": 100.0,
            "min_order_notional_usd": 20.0,
            "session_notional_cap_usd": 120.0,
            "mm_session_notional_done_usd": 80.0,
        }
        client = _FakeClient(mid=100.0)

        def _ok_order(*_args, **kwargs):
            return {"success": True, "digest": "ok"}

        with patch.object(mm_bot, "execute_limit_order", side_effect=_ok_order):
            result = mm_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client, mid=100.0, open_orders=[])

        self.assertTrue(result["success"])
        self.assertAlmostEqual(result["cycle_target_notional_usd"], 40.0, places=6)
        self.assertLessEqual(state.get("mm_session_notional_done_usd", 0), 120.0 + 1e-6)

    def test_session_cap_reached_stops_runtime(self):
        state = {
            "running": True,
            "strategy": "grid",
            "product": "BTC",
            "spread_bp": 4.0,
            "levels": 2,
            "notional_usd": 100.0,
            "cycle_notional_usd": 100.0,
            "min_order_notional_usd": 20.0,
            "session_notional_cap_usd": 200.0,
            "mm_session_notional_done_usd": 200.0,
        }
        client = _FakeClient(mid=100.0)
        result = mm_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client, mid=100.0, open_orders=[])
        self.assertTrue(result["success"])
        self.assertTrue(result.get("done"))
        self.assertFalse(state.get("running"))

    def test_grid_anchor_updates_when_position_changes(self):
        state = {
            "strategy": "rgrid",
            "product": "BTC",
            "rgrid_spread_bp": 10.0,
            "levels": 2,
            "notional_usd": 100.0,
            "cycle_notional_usd": 100.0,
            "min_order_notional_usd": 20.0,
            "rgrid_stop_loss_pct": 5.0,
        }
        client = _FakeClient(
            mid=100.0,
            positions=[{"product_id": 2, "amount": 0.2, "side": "LONG", "unrealized_pnl": 1.0}],
        )
        placed = []

        def _ok_order(*_args, **kwargs):
            placed.append((bool(kwargs.get("is_long")), float(_args[3])))
            return {"success": True, "digest": f"d{len(placed)}"}

        with patch.object(mm_bot, "execute_limit_order", side_effect=_ok_order):
            result = mm_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client, mid=100.0, open_orders=[])

        self.assertTrue(result["success"])
        self.assertEqual(state.get("grid_anchor_price"), 100.0)
        self.assertEqual(state.get("grid_last_fill_price"), 100.0)
        self.assertGreater(result["orders_placed"], 0)

    def test_grid_negative_spread_concedes_exit_for_long_inventory(self):
        state = {
            "strategy": "rgrid",
            "product": "BTC",
            "rgrid_spread_bp": -5.0,
            "levels": 1,
            "notional_usd": 100.0,
            "cycle_notional_usd": 100.0,
            "min_order_notional_usd": 20.0,
            "rgrid_stop_loss_pct": 5.0,
            "grid_anchor_price": 100.0,
            "grid_prev_net_units": 0.2,
        }
        client = _FakeClient(
            mid=100.0,
            positions=[{"product_id": 2, "amount": 0.2, "side": "LONG", "unrealized_pnl": 0.5}],
        )
        quotes = {}

        def _ok_order(*_args, **kwargs):
            quotes["buy" if kwargs.get("is_long") else "sell"] = float(_args[3])
            return {"success": True, "digest": f"d{len(quotes)}"}

        with patch.object(mm_bot, "execute_limit_order", side_effect=_ok_order):
            result = mm_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client, mid=100.0, open_orders=[])

        self.assertTrue(result["success"])
        self.assertGreater(quotes["buy"], state.get("grid_anchor_price"))
        self.assertLess(quotes["sell"], state.get("grid_anchor_price"))
        self.assertGreater(quotes["buy"], quotes["sell"])

    def test_grid_soft_reset_triggers_behind_leg_mid_quote(self):
        state = {
            "strategy": "rgrid",
            "product": "BTC",
            "rgrid_spread_bp": 10.0,
            "levels": 2,
            "notional_usd": 100.0,
            "cycle_notional_usd": 100.0,
            "min_order_notional_usd": 20.0,
            "rgrid_stop_loss_pct": 5.0,
            "rgrid_reset_threshold_pct": 1.0,
            "rgrid_reset_timeout_seconds": 120,
            "grid_anchor_price": 100.0,
            "grid_prev_net_units": 0.2,
        }
        # Long inventory: soft reset arms when spot falls below anchor by >= threshold (here 1%).
        client = _FakeClient(
            mid=98.0,
            positions=[{"product_id": 2, "amount": 0.2, "side": "LONG", "unrealized_pnl": -1.0}],
        )
        sell_quotes = []

        def _ok_order(*_args, **kwargs):
            if not kwargs.get("is_long"):
                sell_quotes.append(float(_args[3]))
            return {"success": True, "digest": f"d{len(sell_quotes)}"}

        with patch.object(mm_bot, "execute_limit_order", side_effect=_ok_order):
            result = mm_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client, mid=98.0, open_orders=[])

        self.assertTrue(result["success"])
        self.assertTrue(state.get("grid_reset_active"))
        self.assertEqual(state.get("grid_reset_side"), "sell")
        self.assertTrue(sell_quotes)
        for q in sell_quotes:
            self.assertLessEqual(q, 98.02)

    def test_grid_pnl_stop_loss_triggers_action(self):
        state = {
            "strategy": "rgrid",
            "product": "BTC",
            "rgrid_spread_bp": 10.0,
            "levels": 2,
            "notional_usd": 1000.0,
            "cycle_notional_usd": 1000.0,
            "min_order_notional_usd": 20.0,
            "rgrid_stop_loss_pct": 1.0,
            "grid_anchor_price": 100.0,
            "grid_prev_net_units": 0.2,
        }
        client = _FakeClient(
            mid=95.0,
            positions=[{"product_id": 2, "amount": 0.2, "side": "LONG", "unrealized_pnl": -15.0}],
        )

        result = mm_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client, mid=95.0, open_orders=[])
        self.assertTrue(result["success"])
        self.assertEqual(result.get("action"), "grid_stop_loss_hit")
        self.assertLessEqual(float(result.get("grid_cycle_pnl_usd") or 0.0), -10.0)

    def test_grid_anchor_does_not_treat_disappeared_quote_as_fill(self):
        state = {
            "strategy": "rgrid",
            "product": "BTC",
            "rgrid_spread_bp": 8.0,
            "levels": 1,
            "notional_usd": 100.0,
            "cycle_notional_usd": 100.0,
            "min_order_notional_usd": 20.0,
            "rgrid_stop_loss_pct": 5.0,
            "grid_anchor_price": 100.0,
            "grid_prev_net_units": 0.0,
            "mm_tracked_quotes": {
                "d-filled": {"digest": "d-filled", "price": 101.25, "is_long": False, "placed_ts": 1.0}
            },
        }
        client = _FakeClient(mid=100.0, positions=[])

        def _ok_order(*_args, **_kwargs):
            return {"success": True, "digest": "d-new"}

        with patch.object(mm_bot, "execute_limit_order", side_effect=_ok_order):
            result = mm_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client, mid=100.0, open_orders=[])

        self.assertTrue(result["success"])
        self.assertAlmostEqual(float(state.get("grid_last_fill_price") or 0.0), 100.0, places=6)
        self.assertAlmostEqual(float(state.get("grid_anchor_price") or 0.0), 100.0, places=6)
        self.assertNotIn("d-filled", state.get("mm_tracked_quotes") or {})


if __name__ == "__main__":
    unittest.main()
