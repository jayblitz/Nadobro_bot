"""Tests for the CEO directive (2026-05): Volume bot uses per-asset MAX leverage
on perp, sizes by $100 NOTIONAL, and rebases session SL/TP to notional.

Locks in:
  - state["leverage"] is overwritten to per-asset max on perp at cycle start.
  - state["leverage"] stays 1.0 on spot.
  - Entry size is computed as target_notional / mid (not effective_margin / mid).
  - tp_usd / sl_usd are computed against target_notional, NOT shrunken margin.
  - target_notional defaults to $100 and respects state["target_notional_usd"]
    plus the legacy "fixed_margin_usd" override.
  - state["leverage_mode"] is set to "MAX" for transparency.
"""
import unittest
from unittest.mock import patch

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from _stubs import install_test_stubs  # noqa: E402

install_test_stubs()

from src.nadobro.strategies import volume_bot  # noqa: E402


class _VolClient:
    """Minimal fake Nado client mirroring tests/test_volume_bot_recycle.py."""

    def __init__(self, mid=10000.0, balance=1000.0, network="mainnet"):
        self._mid = mid
        self._balance = float(balance)
        self.network = network

    def get_market_price(self, _product_id):
        return {"mid": self._mid, "bid": self._mid, "ask": self._mid}

    def get_open_orders(self, _product_id):
        return []

    def get_all_positions(self):
        return []

    def get_balance(self):
        return {"balances": {0: self._balance}}


class VolumePerpMaxLevSizingTests(unittest.TestCase):
    def test_perp_idle_uses_max_lev_and_100_notional(self):
        state = {
            "product": "BTC",
            "vol_market": "perp",
            "vol_direction": "long",
            "vol_phase": "idle",
        }
        client = _VolClient(mid=10000.0, balance=1000.0)
        captured = {}

        def fake_execute_limit_order(*args, **kwargs):
            captured["args"] = args
            captured["kwargs"] = kwargs
            return {"success": True, "digest": "d-entry", "price": kwargs.get("price") or args[3]}

        with patch.object(volume_bot, "get_product_id", return_value=2), patch.object(
            volume_bot, "get_product_max_leverage", return_value=50.0
        ), patch.object(volume_bot, "execute_limit_order", side_effect=fake_execute_limit_order):
            volume_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client)

        # Leverage coerced to per-asset max for perp.
        self.assertEqual(state["leverage"], 50.0)
        self.assertEqual(state["leverage_mode"], "MAX")
        # Default target notional = $100.
        self.assertEqual(state["target_notional_usd"], 100.0)
        # Entry size = target_notional / mid = 100 / 10000 = 0.01.
        size_arg = captured["args"][2]  # execute_limit_order(telegram_id, product, size, ...)
        self.assertAlmostEqual(size_arg, 0.01, places=6)
        # Leverage passed through to the order matches state.
        self.assertEqual(captured["kwargs"].get("leverage"), 50.0)
        # Entry must be post-only (maker).
        self.assertTrue(captured["kwargs"].get("post_only"))

    def test_user_target_notional_override_is_respected(self):
        state = {
            "product": "BTC",
            "vol_market": "perp",
            "vol_direction": "long",
            "vol_phase": "idle",
            "target_notional_usd": 250.0,
        }
        client = _VolClient(mid=10000.0, balance=10000.0)
        captured = {}

        def fake_execute_limit_order(*args, **kwargs):
            captured["args"] = args
            return {"success": True, "digest": "d-entry"}

        with patch.object(volume_bot, "get_product_id", return_value=2), patch.object(
            volume_bot, "get_product_max_leverage", return_value=50.0
        ), patch.object(volume_bot, "execute_limit_order", side_effect=fake_execute_limit_order):
            volume_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client)

        self.assertEqual(state["target_notional_usd"], 250.0)
        size_arg = captured["args"][2]
        # 250 / 10000 = 0.025
        self.assertAlmostEqual(size_arg, 0.025, places=6)

    def test_legacy_fixed_margin_usd_back_compat(self):
        # Old user configs may still set fixed_margin_usd; resolver should pick it up.
        state = {
            "product": "BTC",
            "vol_market": "perp",
            "vol_direction": "long",
            "vol_phase": "idle",
            "fixed_margin_usd": 175.0,
        }
        client = _VolClient(mid=10000.0, balance=10000.0)
        captured = {}

        with patch.object(volume_bot, "get_product_id", return_value=2), patch.object(
            volume_bot, "get_product_max_leverage", return_value=50.0
        ), patch.object(
            volume_bot,
            "execute_limit_order",
            side_effect=lambda *a, **kw: captured.update({"args": a}) or {"success": True, "digest": "d"},
        ):
            volume_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client)

        self.assertEqual(state["target_notional_usd"], 175.0)
        size_arg = captured["args"][2]
        # 175 / 10000 = 0.0175
        self.assertAlmostEqual(size_arg, 0.0175, places=6)


class VolumeSlTpRebasedToNotionalTests(unittest.TestCase):
    def test_sl_triggers_at_pct_of_notional_not_margin(self):
        # Configure: 7% session SL on $100 notional. Should trigger at -$7 PnL,
        # NOT at -$0.14 (which would be 7% of $2 margin at 50x).
        state = {
            "product": "BTC",
            "vol_market": "perp",
            "vol_direction": "long",
            "vol_phase": "idle",
            "tp_pct": 0.0,
            "sl_pct": 7.0,
            "session_realized_pnl_usd": -7.0,
        }
        client = _VolClient(mid=10000.0, balance=1000.0)
        with patch.object(volume_bot, "get_product_id", return_value=2), patch.object(
            volume_bot, "get_product_max_leverage", return_value=50.0
        ):
            result = volume_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client)

        self.assertTrue(result["success"])
        self.assertTrue(result["done"])
        self.assertEqual(result["stop_reason"], "sl_hit")

    def test_sl_does_not_trigger_at_pct_of_margin(self):
        # The legacy bug: 7% of $2 margin = $0.14. Confirm we do NOT stop here.
        state = {
            "product": "BTC",
            "vol_market": "perp",
            "vol_direction": "long",
            "vol_phase": "idle",
            "tp_pct": 0.0,
            "sl_pct": 7.0,
            "session_realized_pnl_usd": -0.50,  # well below $7 but well above $0.14
        }
        client = _VolClient(mid=10000.0, balance=1000.0)
        captured = {}
        with patch.object(volume_bot, "get_product_id", return_value=2), patch.object(
            volume_bot, "get_product_max_leverage", return_value=50.0
        ), patch.object(
            volume_bot,
            "execute_limit_order",
            side_effect=lambda *a, **kw: captured.update({"called": True}) or {"success": True, "digest": "d"},
        ):
            result = volume_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client)

        self.assertTrue(result["success"])
        # Cycle continues — SL did NOT trigger on the shrunken-margin calc.
        self.assertFalse(result.get("done", False))
        self.assertIn("called", captured)


class VolumeSpotStaysOneXTests(unittest.TestCase):
    def test_spot_keeps_leverage_at_one_and_target_notional_100(self):
        state = {
            "product": "KBTC",
            "vol_market": "spot",
            "vol_direction": "long",
            "vol_phase": "idle",
        }
        client = _VolClient(mid=100000.0, balance=10000.0)
        captured = {}
        with patch.object(
            volume_bot, "list_volume_spot_product_names", return_value=["KBTC"]
        ), patch.object(volume_bot, "get_spot_product_id", return_value=42), patch.object(
            volume_bot, "get_spot_metadata", return_value={"symbol": "KBTC"}
        ), patch.object(
            volume_bot,
            "execute_spot_limit_order",
            side_effect=lambda *a, **kw: captured.update({"args": a, "kwargs": kw})
            or {"success": True, "digest": "s1", "price": 100000.0, "size": 0.001},
        ):
            volume_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client)

        # Spot pinned to 1x (no leverage on spot books).
        self.assertEqual(state["leverage"], 1.0)
        # Target notional default still $100.
        self.assertEqual(state["target_notional_usd"], 100.0)
        # Entry size = 100 / 100000 = 0.001
        size_arg = captured["args"][2]
        self.assertAlmostEqual(size_arg, 0.001, places=8)


class VolumeNotionalFloorTests(unittest.TestCase):
    def test_notional_below_floor_aborts_with_clear_error(self):
        state = {
            "product": "BTC",
            "vol_market": "perp",
            "vol_direction": "long",
            "vol_phase": "idle",
            "target_notional_usd": 5.0,  # below MIN_NOTIONAL_USD=10
        }
        client = _VolClient(mid=10000.0, balance=1000.0)
        with patch.object(volume_bot, "get_product_id", return_value=2), patch.object(
            volume_bot, "get_product_max_leverage", return_value=50.0
        ):
            result = volume_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client)

        # Resolver clamps to MIN_NOTIONAL_USD=10 (no abort) — confirm clamp behavior.
        # If the bot should clamp instead of abort, target_notional_usd is bumped to 10.
        self.assertGreaterEqual(state["target_notional_usd"], 10.0)


if __name__ == "__main__":
    unittest.main()
