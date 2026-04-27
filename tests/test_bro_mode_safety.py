import unittest
from unittest.mock import patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.strategies import bro_mode


class BroModeSafetyTests(unittest.TestCase):
    def test_open_blocks_products_outside_configured_allowlist(self):
        result = bro_mode._handle_open(
            telegram_id=1,
            network="mainnet",
            state={"products": ["BTC", "ETH"]},
            decision={"action": "open_long", "product": "SOL", "confidence": 0.9},
            budget=500.0,
            remaining=500.0,
            max_leverage=5,
            max_positions=3,
            bro_settings={"risk_level": "balanced", "budget_usd": 500.0},
        )

        self.assertTrue(result.get("success"))
        self.assertEqual(result.get("action"), "blocked")
        self.assertIn("outside", result.get("detail", ""))

    def test_open_blocks_weak_reward_to_risk(self):
        result = bro_mode._handle_open(
            telegram_id=1,
            network="mainnet",
            state={"products": ["BTC"], "min_reward_risk": 1.5},
            decision={
                "action": "open_long",
                "product": "BTC",
                "confidence": 0.9,
                "expected_pnl_pct": 0.5,
                "sl_pct": 1.0,
            },
            budget=500.0,
            remaining=500.0,
            max_leverage=5,
            max_positions=3,
            bro_settings={"risk_level": "balanced", "budget_usd": 500.0},
        )

        self.assertTrue(result.get("success"))
        self.assertEqual(result.get("action"), "blocked")
        self.assertIn("Reward/risk", result.get("detail", ""))

    def test_close_uses_reduce_only_market_order(self):
        with patch.object(
            bro_mode, "execute_market_order", return_value={"success": True}
        ) as exec_mock, patch.object(bro_mode, "get_product_id", return_value=2):
            result = bro_mode._handle_close(
                telegram_id=1,
                network="mainnet",
                state={"bro_state": {}},
                decision={"product": "BTC", "confidence": 0.8, "reasoning": "exit"},
                positions=[{"product": "BTC", "side": "long", "size": 0.1, "unrealized_pnl": 1.0}],
            )

        self.assertTrue(result.get("success"))
        self.assertTrue(exec_mock.call_args.kwargs.get("reduce_only"))

    def test_emergency_flatten_uses_strategy_network(self):
        with patch("src.nadobro.services.trade_service.close_all_positions", return_value={"success": True}) as close_mock:
            bro_mode._emergency_close_all(telegram_id=1, network="testnet", state={}, products=["BTC"])

        close_mock.assert_called_once_with(1, network="testnet")


if __name__ == "__main__":
    unittest.main()
