import unittest
from unittest.mock import patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.services import x_api_client


class _Resp:
    status_code = 402
    text = '{"account_id":1982353571057176576,"title":"CreditsDepleted"}'

    def json(self):
        return {}


class XApiClientTests(unittest.TestCase):
    def setUp(self):
        x_api_client._credits_depleted_until = 0.0
        x_api_client._credits_depleted_logged = False

    def tearDown(self):
        x_api_client._credits_depleted_until = 0.0
        x_api_client._credits_depleted_logged = False

    def test_credits_depleted_enters_backoff_and_skips_followup_requests(self):
        with patch.dict("os.environ", {"X_API_BEARER_TOKEN": "token"}, clear=False), patch.object(
            x_api_client.requests, "get", return_value=_Resp()
        ) as req:
            first = x_api_client.search_recent_tweets("from:nadoHQ")
            second = x_api_client.search_recent_tweets("from:nadoHQ")

        self.assertEqual(first, [])
        self.assertEqual(second, [])
        self.assertEqual(req.call_count, 1)
        self.assertFalse(x_api_client.is_available())


if __name__ == "__main__":
    unittest.main()
