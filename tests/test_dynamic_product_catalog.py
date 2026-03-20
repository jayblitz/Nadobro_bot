import unittest
from unittest.mock import patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.handlers.intent_parser import parse_trade_intent
from src.nadobro.handlers.keyboards import trade_product_reply_kb
from src.nadobro.services import product_catalog


class DynamicProductCatalogTests(unittest.TestCase):
    def test_parse_trade_intent_recognizes_dynamic_symbol(self):
        with patch("src.nadobro.handlers.intent_parser.get_perp_products", return_value=["BTC", "XAG"]):
            intent = parse_trade_intent("long XAG market with size 2 and leverage 10x", network="mainnet")
        self.assertIsNotNone(intent)
        self.assertEqual(intent.get("product"), "XAG")
        self.assertEqual(intent.get("missing"), [])

    def test_catalog_discovers_dynamic_products(self):
        fake_rows = [
            {"product_id": 2, "symbol": "BTC-PERP", "max_leverage": 40},
            {"product_id": 30, "symbol": "XAG-PERP", "max_leverage": 10},
        ]
        with patch("src.nadobro.services.product_catalog._fetch_all_products", return_value=fake_rows):
            catalog = product_catalog.get_catalog(network="mainnet", refresh=True)
        self.assertIn("XAG", catalog["perps"])
        self.assertEqual(catalog["perps"]["XAG"]["id"], 30)
        self.assertEqual(catalog["perps"]["XAG"]["max_leverage"], 10)
        self.assertEqual(product_catalog.get_product_id("XAG-PERP", network="mainnet"), 30)

    def test_keyboard_uses_dynamic_products(self):
        with patch("src.nadobro.handlers.keyboards._perp_products", return_value=["BTC", "ETH", "XAG"]):
            kb = trade_product_reply_kb(network="mainnet")
        labels = [btn.text for row in kb.keyboard for btn in row]
        self.assertIn("XAG", labels)


if __name__ == "__main__":
    unittest.main()
