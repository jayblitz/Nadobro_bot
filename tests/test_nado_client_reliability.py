import unittest
from unittest.mock import patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.services.nado_client import NadoClient


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
        appendix = NadoClient._build_order_appendix(1, isolated=True, reduce_only=True, margin_x6=12500000)
        self.assertEqual(appendix & 0xFF, 1)  # version
        self.assertEqual((appendix >> 8) & 0x1, 1)  # isolated
        self.assertEqual((appendix >> 9) & 0x3, 1)  # ioc
        self.assertEqual((appendix >> 11) & 0x1, 1)  # reduce_only
        self.assertEqual((appendix >> 64), 12500000)  # margin_x6

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


if __name__ == "__main__":
    unittest.main()
