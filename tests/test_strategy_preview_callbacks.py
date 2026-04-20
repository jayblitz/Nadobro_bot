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

    def test_vol_preview_renders_with_mid_and_limit_entry_text(self):
        fake_user = SimpleNamespace(network_mode=SimpleNamespace(value="mainnet"))
        with patch.object(callbacks, "get_user_settings", return_value=self._settings()), patch.object(
            callbacks, "get_user_readonly_client", return_value=_PreviewClient(mid=123.45, balance=200.0)
        ), patch.object(callbacks, "get_user", return_value=fake_user), patch.object(
            callbacks, "get_product_id", return_value=2
        ), patch.object(
            callbacks, "ensure_active_wallet_ready", return_value=(True, "")
        ), patch.object(
            callbacks, "get_user_wallet_info", return_value={"active_address": "0x1234567890abcdef1234"}
        ), patch.object(
            callbacks, "get_user_bot_status", return_value={}
        ):
            text = callbacks._build_strategy_preview_text(
                telegram_id=1, strategy_id="vol", product="BTC", vol_market="perp"
            )
        self.assertIn("Volume Bot Dashboard", text)
        self.assertIn("Direction: *LONG*", text)
        self.assertIn("Wallet:", text)

    def test_vol_preview_renders_without_client(self):
        with patch.object(callbacks, "get_user_settings", return_value=self._settings()), patch.object(
            callbacks, "get_user_readonly_client", return_value=None
        ), patch.object(
            callbacks, "ensure_active_wallet_ready", return_value=(False, "setup")
        ), patch.object(
            callbacks, "get_user_wallet_info", return_value={}
        ), patch.object(
            callbacks, "get_user_bot_status", return_value={}
        ):
            text = callbacks._build_strategy_preview_text(
                telegram_id=1, strategy_id="vol", product="BTC", vol_market=None
            )
        self.assertIn("Volume Bot Dashboard", text)
        self.assertIn("Wallet: `N/A`", text)

    def test_vol_preview_spot_shows_round_trip_and_spot_label(self):
        fake_user = SimpleNamespace(network_mode=SimpleNamespace(value="mainnet"))
        with patch.object(callbacks, "get_user_settings", return_value=self._settings()), patch.object(
            callbacks, "get_user_readonly_client", return_value=_PreviewClient(mid=50000.0, balance=500.0)
        ), patch.object(callbacks, "get_user", return_value=fake_user), patch.object(
            callbacks, "get_spot_product_id", return_value=77
        ), patch.object(
            callbacks, "ensure_active_wallet_ready", return_value=(True, "")
        ), patch.object(
            callbacks, "get_user_wallet_info", return_value={"active_address": "0x1234567890abcdef1234"}
        ), patch.object(
            callbacks, "get_user_bot_status", return_value={}
        ):
            text = callbacks._build_strategy_preview_text(
                telegram_id=1, strategy_id="vol", product="KBTC", vol_market="spot"
            )
        self.assertIn("KBTC SPOT", text)
        self.assertIn("buy → sell", text)

    def test_dn_preview_uses_dynamic_pair_symbols(self):
        dn_settings = (
            "testnet",
            {
                "default_leverage": 3,
                "strategies": {
                    "dn": {
                        "notional_usd": 50.0,
                        "funding_entry_mode": "enter_anyway",
                        "interval_seconds": 90,
                        "auto_close_on_maintenance": 1,
                    }
                },
            },
        )
        fake_user = SimpleNamespace(network_mode=SimpleNamespace(value="testnet"))
        with patch.object(callbacks, "get_user_settings", return_value=dn_settings), patch.object(
            callbacks, "get_user_readonly_client", return_value=_PreviewClient(mid=123.45, balance=200.0)
        ), patch.object(callbacks, "get_user", return_value=fake_user), patch.object(
            callbacks, "get_product_id", return_value=117
        ), patch.object(
            callbacks, "ensure_active_wallet_ready", return_value=(True, "")
        ), patch.object(
            callbacks, "get_user_wallet_info", return_value={"active_address": "0x1234567890abcdef1234"}
        ), patch.object(
            callbacks,
            "get_dn_pair",
            return_value={
                "product": "WBSPYX",
                "spot_symbol": "WBSPYX",
                "perp_symbol": "WBSPYX-PERP",
                "entry_allowed": True,
                "entry_block_reason": "",
            },
        ), patch.object(
            callbacks, "get_user_bot_status", return_value={}
        ):
            text = callbacks._build_strategy_preview_text(telegram_id=1, strategy_id="dn", product="WBSPYX")
        self.assertIn("Mirror Delta Neutral Dashboard", text)
        self.assertIn("WBSPYX spot / WBSPYX\\-PERP", text)


if __name__ == "__main__":
    unittest.main()
