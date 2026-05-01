import unittest
from unittest.mock import patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.services import portfolio_service


class _Mode:
    value = "mainnet"


class _User:
    network_mode = _Mode()


class _Client:
    def __init__(self):
        self.positions_calls = 0

    def get_all_positions(self):
        self.positions_calls += 1
        return [
            {
                "product_id": 2,
                "product_name": "BTC-PERP",
                "amount": 0.1,
                "signed_amount": 0.1,
                "price": 100000.0,
                "side": "LONG",
            }
        ]

    def get_all_market_prices(self):
        return {"BTC": {"mid": 101000.0}, "AAPL": {"mid": 266.3}}


class PortfolioServiceTests(unittest.TestCase):
    def setUp(self):
        portfolio_service.clear_portfolio_snapshot_cache()

    def tearDown(self):
        portfolio_service.clear_portfolio_snapshot_cache()

    def test_snapshot_uses_live_positions_and_orders(self):
        client = _Client()
        with patch.object(portfolio_service, "get_user", return_value=_User()), patch.object(
            portfolio_service, "get_user_readonly_client", return_value=client
        ), patch.object(
            portfolio_service, "get_trade_analytics", return_value={"total_trades": 1}
        ), patch.object(
            portfolio_service,
            "get_open_limit_orders",
            return_value=[{"product": "AAPL-PERP", "size": 3.0}],
        ):
            snapshot = portfolio_service.get_portfolio_snapshot(42)

        self.assertEqual(snapshot.network, "mainnet")
        self.assertEqual({p["product_name"] for p in snapshot.positions}, {"BTC-PERP"})
        self.assertEqual(snapshot.open_orders[0]["product"], "AAPL-PERP")
        self.assertFalse(snapshot.from_cache)

    def test_snapshot_cache_returns_clone_and_avoids_refetch(self):
        client = _Client()
        with patch.object(portfolio_service, "get_user", return_value=_User()), patch.object(
            portfolio_service, "get_user_readonly_client", return_value=client
        ), patch.object(
            portfolio_service, "get_trade_analytics", return_value={}
        ), patch.object(
            portfolio_service, "get_open_limit_orders", return_value=[]
        ):
            first = portfolio_service.get_portfolio_snapshot(42)
            first.positions.append({"product_name": "MUTATED-PERP"})
            second = portfolio_service.get_portfolio_snapshot(42)

        self.assertEqual(client.positions_calls, 1)
        self.assertTrue(second.from_cache)
        self.assertNotIn("MUTATED-PERP", {p.get("product_name") for p in second.positions})


if __name__ == "__main__":
    unittest.main()
