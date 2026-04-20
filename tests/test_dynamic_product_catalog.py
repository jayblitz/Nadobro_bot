import unittest

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.config import PRODUCTS
from src.nadobro.handlers.intent_parser import parse_trade_intent
from src.nadobro.handlers.keyboards import trade_product_reply_kb
from src.nadobro.services import product_catalog


class DynamicProductCatalogTests(unittest.TestCase):
    def test_dn_underlying_key_maps_xstock_wrapped_symbol(self):
        self.assertEqual(product_catalog._dn_underlying_key("wbNVDA"), "NVDA")
        self.assertEqual(product_catalog._dn_underlying_key("wbAAPL"), "AAPL")
        # Wrapped synthetic ticker keeps legacy WB…X stripping (e.g. SPY perp leg).
        self.assertEqual(product_catalog._dn_underlying_key("WBSPYX"), "SPY")

    def test_parse_trade_intent_recognizes_listed_perp(self):
        names = product_catalog.list_perp_names(network="mainnet", refresh=True)
        self.assertTrue(names, "expected non-empty perp list from catalog")
        sym = names[0]
        intent = parse_trade_intent(
            f"long {sym} market with size 2 and leverage 10x",
            network="mainnet",
        )
        self.assertIsNotNone(intent)
        self.assertEqual(intent.get("product"), sym)
        self.assertEqual(intent.get("missing"), [])

    def test_at_price_implies_limit_order(self):
        intent = parse_trade_intent("Long 2 SOL 5x at 83.0", network="mainnet")
        self.assertIsNotNone(intent)
        self.assertEqual(intent.get("order_type"), "limit")
        self.assertAlmostEqual(intent.get("limit_price"), 83.0)
        self.assertEqual(intent.get("size"), 2.0)
        self.assertEqual(intent.get("missing"), [])

    def test_catalog_loads_from_gateway_or_static_fallback(self):
        catalog = product_catalog.get_catalog(network="mainnet", refresh=True)
        self.assertIn("perps", catalog)
        self.assertGreater(len(catalog["perps"]), 0)
        self.assertIn("BTC", catalog["perps"])
        btc = catalog["perps"]["BTC"]
        self.assertEqual(btc["id"], PRODUCTS["BTC"]["id"])
        self.assertGreater(btc["max_leverage"], 0)
        pid = product_catalog.get_product_id("BTC-PERP", network="mainnet", refresh=True)
        self.assertEqual(pid, PRODUCTS["BTC"]["id"])

    def test_keyboard_lists_live_perp_labels(self):
        kb = trade_product_reply_kb(network="mainnet")
        labels = [btn.text for row in kb.keyboard for btn in row]
        sample = product_catalog.list_perp_names(network="mainnet", refresh=False)[:8]
        self.assertTrue(sample)
        self.assertTrue(any(s in labels for s in sample))

    def test_catalog_resolves_product_id_and_max_leverage(self):
        cap = product_catalog.get_product_max_leverage("BTC", network="mainnet", refresh=True)
        self.assertGreaterEqual(cap, 1)
        pid = product_catalog.get_product_id("BTC", network="mainnet", refresh=True)
        self.assertEqual(pid, PRODUCTS["BTC"]["id"])


if __name__ == "__main__":
    unittest.main()
