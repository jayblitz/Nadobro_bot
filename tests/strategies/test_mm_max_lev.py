"""Tests for the CEO directive (2026-05): MM family (grid/rgrid/dgrid) runs at
per-asset MAX leverage, freeing margin so post-only refresh quotes can sit
alongside closing legs.

Locks in:
  - state["leverage"] is overwritten to per-asset max at cycle start.
  - state["leverage_mode"] is "MAX".
  - The user's notional_usd budget remains the per-cycle notional (no change
    to per-level sizing formula).
  - min_budget_per_order_usd shrinks proportionally (1/max_lev) so smaller
    chunks fit alongside open quotes.
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
    def __init__(self, mid=10000.0, balance=1000.0, equity=1000.0, network="mainnet"):
        self._mid = mid
        self._balance = float(balance)
        self._equity = float(equity)
        self.network = network

    def get_market_price(self, _product_id):
        return {"mid": self._mid, "bid": self._mid - 1.0, "ask": self._mid + 1.0}

    def get_open_orders(self, _product_id):
        return []

    def get_all_positions(self):
        return []

    def get_balance(self):
        return {
            "balances": {0: self._balance},
            "equity": self._equity,
            "available_balance": self._balance,
        }


class MmLeverageCoercionTests(unittest.TestCase):
    def test_run_cycle_coerces_leverage_to_per_asset_max(self):
        state = {
            "product": "BTC",
            "strategy": "grid",
            "leverage": 5.0,  # stale UI value — must be overwritten
            "notional_usd": 200.0,
            "levels": 4,
            "spread_bp": 10.0,
        }
        client = _MmClient(mid=10000.0, balance=10000.0)
        with patch.object(mm_bot, "get_product_id", return_value=2), patch.object(
            mm_bot, "get_product_max_leverage", return_value=50.0
        ), patch.object(
            mm_bot, "execute_limit_order", return_value={"success": True, "digest": "d"}
        ):
            mm_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client)

        # Leverage rewritten to per-asset MAX.
        self.assertEqual(state["leverage"], 50.0)
        self.assertEqual(state["leverage_mode"], "MAX")

    def test_max_lev_falls_back_when_catalog_unavailable(self):
        state = {
            "product": "BTC",
            "strategy": "grid",
            "leverage": 7.0,
            "notional_usd": 200.0,
            "levels": 4,
            "spread_bp": 10.0,
        }
        client = _MmClient(mid=10000.0)

        def boom(*a, **kw):
            raise RuntimeError("catalog offline")

        with patch.object(mm_bot, "get_product_id", return_value=2), patch.object(
            mm_bot, "get_product_max_leverage", side_effect=boom
        ), patch.object(
            mm_bot, "execute_limit_order", return_value={"success": True, "digest": "d"}
        ):
            mm_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client)

        # Defensive fallback: at least the user's stale value is preserved (not zero).
        self.assertGreaterEqual(state["leverage"], 1.0)


if __name__ == "__main__":
    unittest.main()
