"""Phase 5: smoke test that the readiness checker imports cleanly and that
its individual checks compose the expected pass/fail signal.

We don't hit the real Nado gateway here — the live check is operational and
runs against ``gateway.test.nado.xyz``. This test only verifies the wiring of
the script's own helpers so a regression in the dashboard / catalog modules
doesn't silently break the soak preflight.
"""
import importlib.util
import os
import sys
import unittest
from unittest.mock import patch

from _stubs import install_test_stubs

install_test_stubs()


SCRIPT_PATH = os.path.join(
    os.path.dirname(__file__), "..", "scripts", "phase5_readiness_check.py"
)


def _load_script_module():
    spec = importlib.util.spec_from_file_location(
        "phase5_readiness_check", SCRIPT_PATH
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["phase5_readiness_check"] = module
    spec.loader.exec_module(module)
    return module


class ReadinessScriptTests(unittest.TestCase):
    def setUp(self):
        # The locked-builder-fee config refuses to load without these env vars
        # in real deployment; satisfy the validator with a sentinel hex address.
        os.environ["NADO_BUILDER_ID"] = "1"
        self.script = _load_script_module()

    def test_check_builder_fee_locked_passes(self):
        result = self.script.check_builder_fee_locked()
        self.assertTrue(result.ok, result.detail)
        self.assertEqual(result.value["fee_rate"], 10)

    def test_check_min_size_pass_and_fail_paths(self):
        from src.nadobro.services import product_catalog

        # Pass path: positive min_size.
        with patch.object(product_catalog, "get_product_min_quote_notional_usd", return_value=10.0):
            ok_result = self.script.check_catalog_min_size("testnet", "BTC")
        self.assertTrue(ok_result.ok, ok_result.detail)
        self.assertEqual(ok_result.value, 10.0)

        # Fail path: catalog returns None (e.g. fresh testnet, pair not seeded).
        with patch.object(product_catalog, "get_product_min_quote_notional_usd", return_value=None):
            fail_result = self.script.check_catalog_min_size("testnet", "FAKE")
        self.assertFalse(fail_result.ok)

    def test_tiny_budget_math_clears_with_enough_leverage(self):
        from src.nadobro.config import get_product_max_leverage as _gpml  # noqa: F401
        from src.nadobro.services import product_catalog

        with patch.object(self.script, "get_product_max_leverage", create=True):  # noqa: F841
            pass  # placeholder so patch path is exercised

        # $50 collateral × ceil(100/50)=2x → $100 = min_size $100 → ok.
        with patch.object(
            product_catalog, "get_product_min_quote_notional_usd", return_value=100.0
        ), patch(
            "scripts.phase5_readiness_check.get_product_max_leverage", create=True, return_value=10
        ) if False else patch.object(  # use the script's import path directly
            self.script, "check_catalog_max_leverage"
        ):
            # The above cycle is intentionally exercising the patch surface; the
            # real assertion uses the module-level _gpml import inside the
            # script. Easier: patch `get_product_max_leverage` via config.
            from src.nadobro import config
            with patch.object(config, "get_product_max_leverage", return_value=10):
                ok_result = self.script.check_tiny_budget_math("testnet", "BTC", 50.0)
            self.assertTrue(ok_result.ok, ok_result.detail)
            self.assertGreaterEqual(ok_result.value["target_leverage"], 2)

    def test_tiny_budget_math_fails_when_leverage_cap_too_low(self):
        from src.nadobro import config
        from src.nadobro.services import product_catalog

        # min_size $250, collateral $50, max leverage cap 2x → $100 < $250 → FAIL.
        with patch.object(
            product_catalog, "get_product_min_quote_notional_usd", return_value=250.0
        ), patch.object(config, "get_product_max_leverage", return_value=2):
            result = self.script.check_tiny_budget_math("testnet", "BTC", 50.0)
        self.assertFalse(result.ok, result.detail)
        self.assertEqual(result.value["lev_cap"], 2)

    def test_main_returns_nonzero_on_any_failure(self):
        # Force the catalog accessor to return None so check_min_size fails.
        from src.nadobro.services import product_catalog

        with patch.object(product_catalog, "get_product_min_quote_notional_usd", return_value=None), \
             patch("sys.argv", ["phase5_readiness_check", "--network", "testnet", "--product", "BTC", "--json"]):
            rc = self.script.main()
        self.assertEqual(rc, 1)


if __name__ == "__main__":
    unittest.main()
