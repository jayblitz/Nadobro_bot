"""Phase 4 reliability: rate-limit retry helper unit tests."""
import unittest
from unittest.mock import MagicMock

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.services import rate_limit  # noqa: E402


class IsRateLimitErrorTests(unittest.TestCase):
    def test_detects_429_substring(self):
        self.assertTrue(rate_limit.is_rate_limit_error("HTTP 429 Too Many Requests"))

    def test_detects_phrase_variants(self):
        self.assertTrue(rate_limit.is_rate_limit_error("rate limit hit"))
        self.assertTrue(rate_limit.is_rate_limit_error("RateLimit exceeded"))
        self.assertTrue(rate_limit.is_rate_limit_error("client throttled"))

    def test_does_not_match_unrelated_error(self):
        self.assertFalse(rate_limit.is_rate_limit_error("invalid signature"))


class IsTransientErrorTests(unittest.TestCase):
    def test_includes_rate_limit_errors(self):
        self.assertTrue(rate_limit.is_transient_error("429"))

    def test_includes_timeout_and_5xx(self):
        self.assertTrue(rate_limit.is_transient_error("timeout"))
        self.assertTrue(rate_limit.is_transient_error("HTTP 503 Service Unavailable"))
        self.assertTrue(rate_limit.is_transient_error("502 bad gateway"))

    def test_excludes_auth_errors(self):
        self.assertFalse(rate_limit.is_transient_error("401 Unauthorized"))


class CallWithRetryTests(unittest.TestCase):
    def test_returns_first_success_with_no_errors(self):
        fn = MagicMock(return_value={"mid": 100.0})
        value, errors = rate_limit.call_with_retry(fn, max_retries=2)
        self.assertEqual(value, {"mid": 100.0})
        self.assertEqual(errors, [])
        self.assertEqual(fn.call_count, 1)

    def test_retries_on_transient_then_succeeds(self):
        fn = MagicMock(side_effect=[Exception("HTTP 429"), {"mid": 50.0}])
        value, errors = rate_limit.call_with_retry(
            fn, max_retries=2, base_delay=0.0, label="get_market_price"
        )
        self.assertEqual(value, {"mid": 50.0})
        self.assertEqual(len(errors), 1)
        self.assertIn("429", errors[0])
        self.assertEqual(fn.call_count, 2)

    def test_does_not_retry_non_transient_error(self):
        fn = MagicMock(side_effect=ValueError("invalid signature"))
        with self.assertRaises(ValueError):
            rate_limit.call_with_retry(fn, max_retries=3, base_delay=0.0)
        # Hard error → only 1 call.
        self.assertEqual(fn.call_count, 1)

    def test_raises_after_exhausting_retries(self):
        fn = MagicMock(side_effect=Exception("HTTP 429 persistent"))
        with self.assertRaises(Exception) as ctx:
            rate_limit.call_with_retry(fn, max_retries=2, base_delay=0.0)
        self.assertIn("429", str(ctx.exception))
        # max_retries=2 → 3 attempts total.
        self.assertEqual(fn.call_count, 3)

    def test_empty_result_predicate_triggers_retry(self):
        # Sentinel zero-mid response on first call; valid on second.
        fn = MagicMock(side_effect=[
            {"bid": 0, "ask": 0, "mid": 0},
            {"bid": 99, "ask": 101, "mid": 100},
        ])
        value, errors = rate_limit.call_with_retry(
            fn,
            max_retries=2,
            base_delay=0.0,
            is_empty_result=rate_limit.market_price_is_empty,
        )
        self.assertEqual(value, {"bid": 99, "ask": 101, "mid": 100})
        self.assertEqual(len(errors), 1)
        self.assertIn("empty", errors[0])
        self.assertEqual(fn.call_count, 2)

    def test_empty_result_returns_last_value_after_exhaustion(self):
        # All calls return empty — caller still gets the last value back so it
        # can decide what to do (the bot returns its own "could not fetch" error).
        fn = MagicMock(return_value={"mid": 0})
        value, errors = rate_limit.call_with_retry(
            fn, max_retries=1, base_delay=0.0, is_empty_result=rate_limit.market_price_is_empty
        )
        self.assertEqual(value, {"mid": 0})
        # 2 attempts (max_retries=1 means 1 retry, total 2 calls).
        self.assertEqual(fn.call_count, 2)
        self.assertEqual(len(errors), 2)


class MarketPriceEmptyPredicateTests(unittest.TestCase):
    def test_zero_mid_is_empty(self):
        self.assertTrue(rate_limit.market_price_is_empty({"bid": 0, "ask": 0, "mid": 0}))

    def test_positive_mid_is_not_empty(self):
        self.assertFalse(rate_limit.market_price_is_empty({"mid": 100.0}))

    def test_non_dict_is_empty(self):
        self.assertTrue(rate_limit.market_price_is_empty(None))
        self.assertTrue(rate_limit.market_price_is_empty([]))


if __name__ == "__main__":
    unittest.main()
