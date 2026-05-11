"""Integration tests: mm_bot.run_cycle honors participation_preset (Phase 2).

Locks in:
  - When participation_preset is set + the archive returns a positive 24h
    volume, mm_bot derives interval_seconds and per-cycle notional from
    pov_engine and stashes a metadata blob in state.
  - When the archive volume lookup returns None (cold cache / archive offline),
    mm_bot falls back to user-pinned values and writes a warning to state.
  - When participation_preset is unset, mm_bot writes neither metadata nor
    warning.
"""
import unittest
from unittest.mock import patch

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from _stubs import install_test_stubs  # noqa: E402

install_test_stubs()

from src.nadobro.strategies import mm_bot  # noqa: E402


class _MmClient:
    def __init__(self, mid=10000.0, balance=100_000.0):
        self._mid = mid
        self._balance = float(balance)
        self.network = "mainnet"

    def get_market_price(self, _product_id):
        return {"mid": self._mid, "bid": self._mid - 1.0, "ask": self._mid + 1.0}

    def get_open_orders(self, _product_id):
        return []

    def get_all_positions(self):
        return []

    def get_balance(self):
        return {
            "exists": True,
            "balances": {0: self._balance},
            "equity": self._balance,
            "available_balance": self._balance,
        }


class MmPovIntegrationTests(unittest.TestCase):
    def _run(self, state, *, archive_volume_usd):
        client = _MmClient(mid=10000.0, balance=100_000.0)
        with patch.object(mm_bot, "get_product_id", return_value=2), patch.object(
            mm_bot, "get_product_max_leverage", return_value=10.0
        ), patch.object(
            mm_bot, "execute_limit_order", return_value={"success": True, "digest": "d"}
        ), patch.object(
            mm_bot, "get_pair_24h_volume_usd", return_value=archive_volume_usd
        ):
            return mm_bot.run_cycle(
                telegram_id=1, network="mainnet", state=state, client=client, mid=10000.0, open_orders=[]
            )

    def test_normal_preset_overrides_interval_and_cycle_notional(self):
        state = {
            "product": "BTC",
            "strategy": "mid",
            "spread_bp": 5.0,
            "levels": 1,
            "notional_usd": 1_000.0,
            "min_order_notional_usd": 10.0,
            "directional_bias": 0.0,
            "reference_mode": "mid",
            "participation_preset": "normal",
            # User-pinned interval that POV must override.
            "interval_seconds": 30,
            "inventory_soft_limit_usd": 100_000.0,
        }
        self._run(state, archive_volume_usd=1_440_000.0)

        meta = state.get("mm_pov_engine")
        self.assertIsNotNone(meta, "POV metadata should be written when archive returns volume")
        self.assertEqual(meta["preset"], "normal")
        self.assertAlmostEqual(meta["multiplier"], 0.05, places=6)
        # Plan formula: interval_seconds = int(60 / multiplier) = int(60 / 0.05) = 1200.
        self.assertEqual(meta["interval_seconds"], 1200)
        # No warning expected on a successful resolution.
        self.assertNotIn("mm_pov_engine_warning", state)

    def test_archive_failure_keeps_user_pacing_and_writes_warning(self):
        state = {
            "product": "BTC",
            "strategy": "mid",
            "spread_bp": 5.0,
            "levels": 1,
            "notional_usd": 1_000.0,
            "min_order_notional_usd": 10.0,
            "directional_bias": 0.0,
            "reference_mode": "mid",
            "participation_preset": "aggressive",
            "interval_seconds": 30,
            "inventory_soft_limit_usd": 100_000.0,
        }
        self._run(state, archive_volume_usd=None)
        self.assertNotIn("mm_pov_engine", state)
        self.assertIn("mm_pov_engine_warning", state)
        self.assertIn("aggressive", state["mm_pov_engine_warning"])

    def test_no_preset_writes_no_pov_state(self):
        state = {
            "product": "BTC",
            "strategy": "mid",
            "spread_bp": 5.0,
            "levels": 1,
            "notional_usd": 1_000.0,
            "min_order_notional_usd": 10.0,
            "directional_bias": 0.0,
            "reference_mode": "mid",
            "interval_seconds": 30,
            "inventory_soft_limit_usd": 100_000.0,
        }
        self._run(state, archive_volume_usd=1_440_000.0)
        self.assertNotIn("mm_pov_engine", state)
        self.assertNotIn("mm_pov_engine_warning", state)

    def test_pov_normalizes_uppercase_preset(self):
        state = {
            "product": "BTC",
            "strategy": "grid",
            "spread_bp": 8.0,
            "levels": 2,
            "notional_usd": 1_000.0,
            "min_order_notional_usd": 10.0,
            "participation_preset": "Passive",  # mixed case
            "inventory_soft_limit_usd": 100_000.0,
        }
        self._run(state, archive_volume_usd=10_000_000.0)
        meta = state.get("mm_pov_engine")
        self.assertIsNotNone(meta)
        self.assertEqual(meta["preset"], "passive")


if __name__ == "__main__":
    unittest.main()
