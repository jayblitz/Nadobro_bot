import unittest
from types import SimpleNamespace
from unittest.mock import patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.handlers import callbacks


class _PreviewClient:
    def __init__(self, mid=0.0, balance=0.0):
        self._mid = float(mid)
        self._balance = float(balance)

    def get_balance(self):
        return {"exists": True, "balances": {0: self._balance}}

    def get_market_price(self, _pid):
        return {"mid": self._mid}

    def get_funding_rate(self, _pid):
        return {"funding_rate": 0.0}


class StrategyPreviewCallbackTests(unittest.TestCase):
    def _settings(self):
        return (
            "mainnet",
            {
                "default_leverage": 3,
                "slippage": 1,
                "strategies": {
                    "vol": {
                        "tp_pct": 1.0,
                        "sl_pct": 1.0,
                        "vol_direction": "long",
                        "interval_seconds": 10,
                    }
                },
            },
        )

    def test_vol_preview_renders_with_mid_and_market_entry_text(self):
        fake_user = SimpleNamespace(network_mode=SimpleNamespace(value="mainnet"))
        with patch.object(callbacks, "get_user_settings", return_value=self._settings()), patch.object(
            callbacks, "get_user_readonly_client", return_value=_PreviewClient(mid=123.45, balance=200.0)
        ), patch.object(callbacks, "get_user", return_value=fake_user), patch.object(
            callbacks, "get_product_id", return_value=2
        ), patch.object(
            callbacks, "get_user_bot_status", return_value={}
        ):
            text = callbacks._build_strategy_preview_text(telegram_id=1, strategy_id="vol", product="BTC")
        self.assertIn("Volume Bot Dashboard", text)
        self.assertIn("Entry *Market*", text)
        self.assertIn("Mode *MAINNET*", text)

    def test_vol_preview_renders_without_client(self):
        with patch.object(callbacks, "get_user_settings", return_value=self._settings()), patch.object(
            callbacks, "get_user_readonly_client", return_value=None
        ), patch.object(
            callbacks, "get_user_bot_status", return_value={}
        ):
            text = callbacks._build_strategy_preview_text(telegram_id=1, strategy_id="vol", product="BTC")
        self.assertIn("Volume Bot Dashboard", text)
        self.assertIn("Mid *N/A*", text)


if __name__ == "__main__":
    unittest.main()
