import sys
import types
import unittest
from unittest.mock import patch


# Lightweight psycopg2 stubs for import-only test environments.
if "psycopg2" not in sys.modules:
    psycopg2_mod = types.ModuleType("psycopg2")
    psycopg2_pool = types.ModuleType("psycopg2.pool")
    psycopg2_extras = types.ModuleType("psycopg2.extras")
    psycopg2_sql = types.ModuleType("psycopg2.sql")

    class _ThreadedConnectionPool:
        def __init__(self, *args, **kwargs):
            pass

    class _RealDictCursor:
        pass

    class _SqlFragment:
        def __init__(self, value=""):
            self.value = value

        def format(self, *args, **kwargs):
            return self

        def join(self, _iterable):
            return self

        def __mul__(self, _):
            return self

    def _sql_factory(value=""):
        return _SqlFragment(value)

    def _identifier_factory(_value=""):
        return _SqlFragment("")

    def _placeholder_factory():
        return _SqlFragment("%s")

    psycopg2_pool.ThreadedConnectionPool = _ThreadedConnectionPool
    psycopg2_extras.RealDictCursor = _RealDictCursor
    psycopg2_sql.SQL = _sql_factory
    psycopg2_sql.Identifier = _identifier_factory
    psycopg2_sql.Placeholder = _placeholder_factory

    psycopg2_mod.pool = psycopg2_pool
    psycopg2_mod.extras = psycopg2_extras
    psycopg2_mod.sql = psycopg2_sql

    sys.modules["psycopg2"] = psycopg2_mod
    sys.modules["psycopg2.pool"] = psycopg2_pool
    sys.modules["psycopg2.extras"] = psycopg2_extras
    sys.modules["psycopg2.sql"] = psycopg2_sql

# Lightweight requests stub for import-only environments.
if "requests" not in sys.modules:
    requests_mod = types.ModuleType("requests")

    class _DummyResponse:
        def json(self):
            return {}

    class _DummySession:
        def get(self, *args, **kwargs):
            return _DummyResponse()

    requests_mod.Session = _DummySession
    sys.modules["requests"] = requests_mod

from src.nadobro.strategies import mm_bot


class _FakeClient:
    def __init__(self, mid=100.0, positions=None, open_orders=None):
        self._mid = mid
        self._positions = positions or []
        self._open_orders = open_orders or []
        self.cancelled = []

    def get_market_price(self, _product_id):
        return {"mid": self._mid}

    def get_all_positions(self):
        return list(self._positions)

    def get_open_orders(self, _product_id):
        return list(self._open_orders)

    def cancel_order(self, _product_id, digest):
        self.cancelled.append(digest)
        self._open_orders = [o for o in self._open_orders if o.get("digest") != digest]
        return {"success": True}


class MmProfitabilityTests(unittest.TestCase):
    def test_reference_mode_and_volatility_adjust_spread(self):
        state = {
            "strategy": "mm",
            "product": "BTC",
            "spread_bp": 4.0,
            "levels": 2,
            "reference_mode": "ema_fast",
            "ema_fast_alpha": 0.5,
            "mm_ref_ema_fast": 100.0,
            "vol_window_points": 6,
            "vol_sensitivity": 0.2,
            "min_spread_bp": 2.0,
            "max_spread_bp": 30.0,
            "notional_usd": 100.0,
            "cycle_notional_usd": 100.0,
            "mm_mid_history": [100, 110, 90, 112, 88],
        }
        client = _FakeClient(mid=105.0)
        placed = []

        def _ok_order(*_args, **kwargs):
            placed.append(kwargs.get("price"))
            idx = len(placed)
            return {"success": True, "digest": f"d{idx}"}

        with patch.object(mm_bot, "execute_limit_order", side_effect=_ok_order):
            result = mm_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client, mid=105.0, open_orders=[])

        self.assertTrue(result["success"])
        self.assertGreater(result["spread_bp"], 4.0)
        self.assertNotEqual(round(result["reference_price"], 8), 105.0)
        self.assertGreater(result["orders_placed"], 0)
        self.assertIn("mm_last_metrics", state)

    def test_hard_inventory_limit_flattens_only_one_side(self):
        state = {
            "strategy": "mm",
            "product": "BTC",
            "spread_bp": 4.0,
            "levels": 2,
            "notional_usd": 100.0,
            "cycle_notional_usd": 100.0,
            "inventory_soft_limit_usd": 20.0,
        }
        # Long inventory ($50) breaches hard limit (~$36+), so only sell quotes should be placed.
        client = _FakeClient(
            mid=100.0,
            positions=[{"product_id": 2, "amount": 0.5, "side": "LONG"}],
        )
        sides = []

        def _ok_order(*_args, **kwargs):
            sides.append(bool(kwargs.get("is_long")))
            return {"success": True, "digest": f"d{len(sides)}"}

        with patch.object(mm_bot, "execute_limit_order", side_effect=_ok_order):
            result = mm_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client, mid=100.0, open_orders=[])

        self.assertTrue(result["success"])
        self.assertTrue(state.get("mm_paused"))
        self.assertTrue(state.get("mm_pause_reason"))
        self.assertTrue(sides)  # At least one order was attempted
        self.assertTrue(all(side is False for side in sides))  # False = sell side only

    def test_twap_cycle_budget_respects_session_cap(self):
        state = {
            "strategy": "mm",
            "product": "BTC",
            "spread_bp": 4.0,
            "levels": 2,
            "notional_usd": 100.0,
            "cycle_notional_usd": 100.0,
            "session_notional_cap_usd": 120.0,
            "mm_session_notional_done_usd": 80.0,
        }
        client = _FakeClient(mid=100.0)

        def _ok_order(*_args, **kwargs):
            return {"success": True, "digest": "ok"}

        with patch.object(mm_bot, "execute_limit_order", side_effect=_ok_order):
            result = mm_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client, mid=100.0, open_orders=[])

        self.assertTrue(result["success"])
        self.assertAlmostEqual(result["cycle_target_notional_usd"], 40.0, places=6)
        self.assertLessEqual(state.get("mm_session_notional_done_usd", 0), 120.0 + 1e-6)

    def test_session_cap_reached_stops_runtime(self):
        state = {
            "running": True,
            "strategy": "mm",
            "product": "BTC",
            "spread_bp": 4.0,
            "levels": 2,
            "notional_usd": 100.0,
            "cycle_notional_usd": 100.0,
            "session_notional_cap_usd": 200.0,
            "mm_session_notional_done_usd": 200.0,
        }
        client = _FakeClient(mid=100.0)
        result = mm_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client, mid=100.0, open_orders=[])
        self.assertTrue(result["success"])
        self.assertTrue(result.get("done"))
        self.assertFalse(state.get("running"))


if __name__ == "__main__":
    unittest.main()
