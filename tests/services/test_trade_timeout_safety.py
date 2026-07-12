import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.models.database import TradeStatus
from src.nadobro.trading import trade_service


class _Client:
    def get_market_price(self, _product_id):
        return {"mid": 100.0}

    def place_market_order(self, *args, **kwargs):
        raise AssertionError("submit is patched in these tests")


class TradeTimeoutSafetyTests(unittest.TestCase):
    def test_market_submit_timeout_keeps_trade_pending_unknown(self):
        updates = []

        with patch.object(trade_service, "_builder_route_payload", return_value={}), patch.object(
            trade_service, "validate_trade", return_value=(True, "")
        ), patch.object(
            trade_service,
            "get_user",
            return_value=SimpleNamespace(network_mode=SimpleNamespace(value="mainnet")),
        ), patch.object(trade_service, "get_product_id", return_value=1), patch.object(
            trade_service, "get_product_name", return_value="BTC-PERP"
        ), patch.object(trade_service, "get_user_nado_client", return_value=_Client()), patch.object(
            trade_service, "get_trades_by_user", return_value=[]
        ), patch.object(trade_service, "insert_trade", return_value=99), patch.object(
            trade_service, "update_trade", side_effect=lambda *args, **kwargs: updates.append((args, kwargs))
        ), patch.object(
            trade_service, "_submit_with_timeout", return_value=(False, "Order submit timed out after 25s")
        ), patch(
            "src.nadobro.trading.order_intents.reserve_order_intent",
            return_value=(True, {"intent_id": "intent"}),
        ), patch(
            "src.nadobro.trading.order_intents.update_order_intent"
        ):
            result = trade_service.execute_market_order(
                telegram_id=42,
                product="BTC",
                size=1.0,
                is_long=True,
                source="manual",
            )

        self.assertFalse(result["success"])
        self.assertTrue(result["pending"])
        self.assertEqual(result["trade_id"], 99)
        update_payload = updates[0][0][1]
        self.assertEqual(update_payload["status"], TradeStatus.PENDING.value)
        self.assertTrue(update_payload["error_message"].startswith("Order submit status unknown"))

    def test_recent_unknown_market_submit_blocks_matching_retry(self):
        recent_unknown = {
            "id": 77,
            "product_id": 1,
            "status": TradeStatus.PENDING.value,
            "order_type": "market",
            "side": "long",
            "size": 1.0,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "error_message": "Order submit status unknown: timeout",
        }

        with patch.object(trade_service, "_builder_route_payload", return_value={}), patch.object(
            trade_service, "validate_trade", return_value=(True, "")
        ), patch.object(
            trade_service,
            "get_user",
            return_value=SimpleNamespace(network_mode=SimpleNamespace(value="mainnet")),
        ), patch.object(trade_service, "get_product_id", return_value=1), patch.object(
            trade_service, "get_user_nado_client", return_value=_Client()
        ), patch.object(trade_service, "get_trades_by_user", return_value=[recent_unknown]), patch.object(
            trade_service, "insert_trade"
        ) as insert_spy:
            result = trade_service.execute_market_order(
                telegram_id=42,
                product="BTC",
                size=1.0,
                is_long=True,
                source="manual",
            )

        self.assertFalse(result["success"])
        self.assertTrue(result["pending"])
        self.assertEqual(result["trade_id"], 77)
        insert_spy.assert_not_called()


if __name__ == "__main__":
    unittest.main()
