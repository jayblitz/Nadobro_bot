import unittest
from types import SimpleNamespace
from unittest.mock import patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.services import user_service


class _SigningClient:
    def get_market_price(self, _product_id):
        return {"mid": 95_000.0}


class PreflightVolSpotTests(unittest.TestCase):
    def test_run_strategy_start_preflight_spot_kbtc_ok(self):
        with patch.object(user_service, "ensure_active_wallet_ready", return_value=(True, "")), patch.object(
            user_service, "get_user_nado_client", return_value=_SigningClient()
        ), patch.object(user_service, "get_nado_builder_routing_config", return_value=(1, 1)), patch(
            "src.nadobro.config.get_spot_product_id", return_value=501
        ):
            ok, msg = user_service.run_strategy_start_preflight(
                telegram_id=1,
                product="KBTC",
                network="mainnet",
                vol_market="spot",
            )
        self.assertTrue(ok)
        self.assertEqual(msg, "")


if __name__ == "__main__":
    unittest.main()
