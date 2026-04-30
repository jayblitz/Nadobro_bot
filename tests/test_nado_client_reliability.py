import unittest
from unittest.mock import patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.services.nado_client import NadoClient
from src.nadobro.services.trade_service import _normalize_net_positions


class _FakeResponse:
    def __init__(self, payload=None, raise_json=False, text="", status_code=200):
        self._payload = payload or {}
        self._raise_json = raise_json
        self.text = text
        self.status_code = status_code
        self.headers = {"content-type": "application/json" if not raise_json else "text/html"}

    def json(self):
        if self._raise_json:
            raise ValueError("not json")
        return self._payload


class NadoClientReliabilityTests(unittest.TestCase):
    def test_build_order_appendix_sets_isolated_reduce_only_and_margin(self):
        appendix = NadoClient._build_order_appendix(
            1,
            isolated=True,
            reduce_only=True,
            margin_x6=12500000,
            builder_id=4321,
            builder_fee_rate=10,
        )
        self.assertEqual(appendix & 0xFF, 1)  # version
        self.assertEqual((appendix >> 8) & 0x1, 1)  # isolated
        self.assertEqual((appendix >> 9) & 0x3, 1)  # ioc
        self.assertEqual((appendix >> 11) & 0x1, 1)  # reduce_only
        self.assertEqual((appendix >> 38) & 0x3FF, 10)  # 1 bps fee rate
        self.assertEqual((appendix >> 48) & 0xFFFF, 4321)  # builder id
        self.assertEqual((appendix >> 64), 12500000)  # margin_x6

    def test_place_order_hard_fails_when_builder_id_missing(self):
        client = NadoClient(private_key="0xabc", network="mainnet")
        client._initialized = True
        client.client = object()
        with patch.dict("os.environ", {}, clear=True):
            result = client.place_order(product_id=2, size=0.1, price=100000.0, is_buy=True)
        self.assertEqual(result.get("success"), False)
        self.assertIn("NADO_BUILDER_ID", result.get("error", ""))

    def test_place_order_hard_fails_when_builder_id_is_zero(self):
        client = NadoClient(private_key="0xabc", network="mainnet")
        client._initialized = True
        client.client = object()
        with patch.dict("os.environ", {"NADO_BUILDER_ID": "0", "NADO_BUILDER_FEE_RATE": "10"}, clear=True):
            result = client.place_order(product_id=2, size=0.1, price=100000.0, is_buy=True)
        self.assertEqual(result.get("success"), False)
        self.assertIn("NADO_BUILDER_ID", result.get("error", ""))

    def test_place_order_hard_fails_when_builder_id_out_of_range(self):
        client = NadoClient(private_key="0xabc", network="mainnet")
        client._initialized = True
        client.client = object()
        with patch.dict("os.environ", {"NADO_BUILDER_ID": "70000", "NADO_BUILDER_FEE_RATE": "10"}, clear=True):
            result = client.place_order(product_id=2, size=0.1, price=100000.0, is_buy=True)
        self.assertEqual(result.get("success"), False)
        self.assertIn("NADO_BUILDER_ID", result.get("error", ""))

    def test_place_order_hard_fails_when_builder_fee_rate_not_one_bps(self):
        client = NadoClient(private_key="0xabc", network="mainnet")
        client._initialized = True
        client.client = object()
        with patch.dict("os.environ", {"NADO_BUILDER_ID": "123", "NADO_BUILDER_FEE_RATE": "11"}, clear=True):
            result = client.place_order(product_id=2, size=0.1, price=100000.0, is_buy=True)
        self.assertEqual(result.get("success"), False)
        self.assertIn("NADO_BUILDER_FEE_RATE", result.get("error", ""))

    def test_query_rest_retries_after_non_json_response(self):
        client = NadoClient(private_key="0xabc", network="mainnet")
        responses = [
            _FakeResponse(raise_json=True, text="<html>error</html>", status_code=502),
            _FakeResponse(payload={"status": "success", "data": {"ok": True}}),
        ]

        with patch("src.nadobro.services.nado_client._rest_session.get", side_effect=responses), patch(
            "src.nadobro.services.nado_client.time.sleep", return_value=None
        ):
            data = client._query_rest("symbols")

        self.assertIsNotNone(data)
        self.assertEqual(data.get("status"), "success")
        self.assertEqual(data.get("data", {}).get("ok"), True)

    def test_query_rest_uses_post_for_multi_product_queries(self):
        client = NadoClient(private_key="0xabc", network="mainnet")
        response = _FakeResponse(payload={"status": "success", "data": {"market_prices": []}})
        with patch("src.nadobro.services.nado_client._rest_session.get") as mock_get, patch(
            "src.nadobro.services.nado_client._rest_session.post", return_value=response
        ) as mock_post:
            data = client._query_rest("market_prices", {"product_ids": [1, 2]})

        self.assertEqual(data.get("status"), "success")
        mock_get.assert_not_called()
        mock_post.assert_called_once()

    def test_extract_positions_from_rest_payload_supports_camel_case_fields(self):
        client = NadoClient(private_key="0xabc", network="mainnet")
        payload = {
            "perpPositions": [
                {
                    "productId": 999,
                    "balance": {
                        "amountX18": "3000000000000000000",
                        "vQuoteBalanceX18": "180000000000000000000",
                    },
                    "entryPriceX18": "60000000000000000000",
                    "isLong": True,
                }
            ]
        }
        positions = client._extract_positions_from_rest_payload(payload)
        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0]["product_id"], 999)
        self.assertEqual(positions[0]["side"], "LONG")
        self.assertGreater(positions[0]["amount"], 0)

    def test_get_all_positions_unwraps_nested_subaccount_info_payload(self):
        client = NadoClient(private_key="0xabc", network="mainnet")
        client._initialized = False
        client.client = None
        with patch.object(
            client,
            "_query_rest",
            return_value={
                "status": "success",
                "data": {
                    "subaccountInfo": {
                        "perpPositions": [
                            {
                                "productId": 111,
                                "balance": {"amountX18": "1000000000000000000"},
                                "entryPriceX18": "10000000000000000000",
                                "side": "LONG",
                            }
                        ]
                    }
                },
            },
        ):
            positions = client.get_all_positions()
        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0]["product_id"], 111)

    def test_extract_positions_from_rest_payload_supports_balance_amount_fields(self):
        client = NadoClient(private_key="0xabc", network="mainnet")
        payload = {
            "perpBalances": [
                {
                    "productId": 222,
                    "balance": {
                        "balanceAmountX18": "-6000000000000000000",
                        "vQuoteBalanceX18": "-617880000000000000000",
                    },
                    "entryPriceX18": "102980000000000000000",
                    "side": "SHORT",
                }
            ]
        }
        positions = client._extract_positions_from_rest_payload(payload)
        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0]["product_id"], 222)
        self.assertEqual(positions[0]["side"], "SHORT")
        self.assertAlmostEqual(float(positions[0]["amount"]), 6.0, places=6)
        self.assertAlmostEqual(float(positions[0]["signed_amount"]), -6.0, places=6)

    def test_extract_positions_from_sdk_info_unwraps_nested_container(self):
        client = NadoClient(private_key="0xabc", network="mainnet")

        class _Balance:
            amountX18 = "-2000000000000000000"

        class _Position:
            productId = 333
            balance = _Balance()
            entryPriceX18 = "50000000000000000000"
            side = "SHORT"

        class _Container:
            perpBalances = [_Position()]

        class _Wrapper:
            subaccount_info = _Container()

        positions = client._extract_positions_from_sdk_info(_Wrapper())
        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0]["product_id"], 333)
        self.assertEqual(positions[0]["side"], "SHORT")
        self.assertAlmostEqual(float(positions[0]["signed_amount"]), -2.0, places=6)

    def test_normalize_net_positions_uses_signed_amount_when_amount_missing(self):
        net = _normalize_net_positions(
            [
                {
                    "product_id": 444,
                    "product_name": "WTI-PERP",
                    "signed_amount": -6.0,
                    "amount": 0.0,
                    "side": "",
                    "price": 103.0,
                }
            ]
        )
        self.assertIn(444, net)
        self.assertAlmostEqual(float(net[444]["signed_amount"]), -6.0, places=6)

    def test_normalize_net_positions_splits_isolated_subaccounts(self):
        net = _normalize_net_positions(
            [
                {
                    "product_id": 1,
                    "product_name": "AAPL-PERP",
                    "signed_amount": 0.5,
                    "amount": 0.5,
                    "side": "LONG",
                    "price": 200.0,
                    "subaccount": "0xiso1",
                },
                {
                    "product_id": 1,
                    "product_name": "AAPL-PERP",
                    "signed_amount": 0.2,
                    "amount": 0.2,
                    "side": "LONG",
                    "price": 199.0,
                    "subaccount": "0xiso2",
                },
            ]
        )
        self.assertIn(1, net)
        self.assertIn("legs", net[1])
        self.assertEqual(len(net[1]["legs"]), 2)

    def test_isolated_subaccounts_response_parsing(self):
        from src.nadobro.services.nado_archive import (
            _isolated_subaccounts_list_from_response,
            isolated_subaccount_from_row,
            query_isolated_subaccounts_for_parent,
        )

        rows = _isolated_subaccounts_list_from_response(
            {"isolated_subaccounts": [{"isolated_subaccount": "0xabc", "product_id": 9}]}
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].get("product_id"), 9)
        self.assertEqual(isolated_subaccount_from_row({"subaccountHex": "0xchild"}, "0xparent"), "0xchild")
        self.assertEqual(isolated_subaccount_from_row({"subaccount": "0xparent"}, "0xparent"), "")

        captured = {}

        def _fake_post(_url, payload):
            captured["payload"] = payload
            return {"isolated_subaccounts": []}

        with patch("src.nadobro.services.nado_archive._post", side_effect=_fake_post):
            query_isolated_subaccounts_for_parent("mainnet", "0xparent")

        self.assertEqual(captured["payload"]["isolated_subaccounts"]["subaccount"], "0xparent")

    def test_archive_unwraps_orders_under_data(self):
        from src.nadobro.services.nado_archive import _orders_list_from_archive_response

        row = {"digest": "0xabc", "base_filled": 1e18, "quote_filled": 100e18}
        self.assertEqual(
            _orders_list_from_archive_response({"data": {"orders": [row]}}),
            [row],
        )

    def test_parse_order_reads_camelCase_fill_fields(self):
        from src.nadobro.services.nado_archive import _parse_order

        o = _parse_order(
            {
                "digest": "0x1",
                "baseFilled": 5e18,
                "quoteFilled": 500e18,
            }
        )
        self.assertTrue(o["is_filled"])
        self.assertGreater(o["fill_size"], 0)


if __name__ == "__main__":
    unittest.main()
