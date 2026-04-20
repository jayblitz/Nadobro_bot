import asyncio
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.services import bot_runtime
from src.nadobro.strategies import volume_bot


class _VolClient:
    def __init__(
        self,
        mid=100.0,
        open_orders=None,
        positions=None,
        network="mainnet",
        balance=1000.0,
        spot_base_by_id=None,
    ):
        self._mid = mid
        self._open_orders = open_orders or []
        self._positions = positions or []
        self._balance = float(balance)
        self._spot_base = dict(spot_base_by_id or {})
        self.network = network

    def get_market_price(self, _product_id):
        return {"mid": self._mid, "bid": self._mid, "ask": self._mid}

    def get_open_orders(self, _product_id):
        return list(self._open_orders)

    def get_all_positions(self):
        return list(self._positions)

    def get_balance(self):
        b = {0: self._balance}
        for k, v in self._spot_base.items():
            b[k] = v
            b[str(k)] = v
        return {"balances": b}


class VolStrategyRebuildTests(unittest.TestCase):
    def test_vol_idle_places_limit_entry_with_fixed_margin(self):
        state = {"product": "BTC", "vol_direction": "long", "tp_pct": 1.0, "sl_pct": 1.0}
        client = _VolClient(mid=100.0)
        with patch.object(volume_bot, "get_product_id", return_value=2), patch.object(
            volume_bot,
            "execute_limit_order",
            return_value={"success": True, "digest": "d1", "price": 100.1, "size": 1.0},
        ):
            result = volume_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client)
        self.assertTrue(result.get("success"))
        self.assertEqual(result.get("action"), "opened_limit_wait_fill")
        self.assertEqual(result.get("vol_order_attempts"), 1)
        self.assertEqual(result.get("vol_order_failures"), 0)
        self.assertEqual(state.get("vol_phase"), "pending_fill")
        self.assertAlmostEqual(float(state.get("vol_entry_fill_price") or 0), 100.1)
        self.assertEqual(float(state.get("vol_entry_fill_ts") or 0), 0.0)
        self.assertEqual(state.get("leverage"), 1.0)
        self.assertEqual(state.get("fixed_margin_usd"), 100.0)

    def test_vol_pending_fill_waits_while_order_is_open(self):
        state = {
            "product": "BTC",
            "vol_phase": "pending_fill",
            "vol_entry_digest": "d-open",
            "vol_direction": "short",
        }
        client = _VolClient(open_orders=[{"digest": "d-open"}], positions=[])
        with patch.object(volume_bot, "get_product_id", return_value=2):
            result = volume_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client)
        self.assertTrue(result.get("success"))
        self.assertEqual(result.get("action"), "waiting_entry_fill")

    def test_vol_pending_fill_transitions_to_wait_close_after_fill(self):
        state = {
            "product": "BTC",
            "vol_phase": "pending_fill",
            "vol_entry_digest": "d-filled",
            "vol_direction": "long",
        }
        client = _VolClient(
            open_orders=[],
            positions=[{"product_id": 2, "amount": 1.0, "side": "LONG"}],
        )
        with patch.object(volume_bot, "get_product_id", return_value=2), patch.object(
            volume_bot, "query_order_by_digest", return_value={"fill_price": 101.0, "fill_size": 1.0}
        ):
            result = volume_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client)
        self.assertTrue(result.get("success"))
        self.assertEqual(result.get("action"), "entry_filled_wait_close")
        self.assertEqual(state.get("vol_phase"), "filled_wait_close")
        self.assertGreater(float(state.get("vol_entry_fill_ts") or 0), 0)

    def test_vol_close_after_60s_places_limit_close_then_waits_fill(self):
        state = {
            "product": "BTC",
            "vol_phase": "filled_wait_close",
            "vol_direction": "long",
            "vol_entry_fill_ts": time.time() - 61.0,
            "vol_entry_fill_price": 100.0,
            "vol_entry_size": 1.0,
            "tp_pct": 1.0,
            "sl_pct": 1.0,
            "session_realized_pnl_usd": 0.0,
        }
        client = _VolClient(
            mid=100.0,
            open_orders=[],
            positions=[{"product_id": 2, "amount": 1.0, "side": "LONG"}],
        )
        with patch.object(volume_bot, "get_product_id", return_value=2), patch.object(
            volume_bot, "execute_limit_order", return_value={"success": True, "digest": "d-close", "price": 99.9}
        ):
            result = volume_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client)
        self.assertTrue(result.get("success"))
        self.assertFalse(result.get("done"))
        self.assertEqual(result.get("action"), "placed_limit_close_wait_fill")
        self.assertEqual(state.get("vol_phase"), "pending_close_fill")
        self.assertEqual(state.get("vol_close_digest"), "d-close")

    def test_vol_pending_close_fill_realizes_and_stops_on_tp(self):
        state = {
            "product": "BTC",
            "vol_phase": "pending_close_fill",
            "vol_direction": "long",
            "vol_entry_fill_price": 100.0,
            "vol_entry_size": 1.0,
            "vol_close_digest": "d-close",
            "vol_close_size": 1.0,
            "tp_pct": 1.0,
            "sl_pct": 1.0,
            "session_realized_pnl_usd": 1.2,
        }
        client = _VolClient(mid=101.0, open_orders=[], positions=[])
        with patch.object(volume_bot, "get_product_id", return_value=2), patch.object(
            volume_bot, "query_order_by_digest", return_value={"fill_price": 101.0, "realized_pnl": 1.2, "fee": 0.0}
        ):
            result = volume_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client)
        self.assertTrue(result.get("success"))
        self.assertTrue(result.get("done"))
        self.assertEqual(result.get("stop_reason"), "tp_hit")
        self.assertFalse(state.get("running", True))

    def test_runtime_finalizes_vol_session_on_stop_reason(self):
        telegram_id = 99
        network = "mainnet"
        state = {
            "running": True,
            "strategy": "vol",
            "product": "BTC",
            "tp_pct": 1.0,
            "sl_pct": 1.0,
            "interval_seconds": 10,
            "last_run_ts": 0.0,
            "strategy_session_id": 10,
        }
        fake_user = SimpleNamespace(network_mode=SimpleNamespace(value=network))
        saved_states = []

        class _RuntimeClient:
            def get_market_price(self, _product_id):
                return {"mid": 100.0}

            def get_open_orders(self, _product_id):
                return []

        async def _run_blocking_stub(func, *args, **kwargs):
            return func(*args, **kwargs)

        def _save_state_stub(_uid, _network, new_state):
            saved_states.append(dict(new_state))

        with patch.object(bot_runtime, "is_trading_paused", return_value=False), patch.object(
            bot_runtime, "run_blocking", side_effect=_run_blocking_stub
        ), patch.object(bot_runtime, "get_user", return_value=fake_user), patch.object(
            bot_runtime, "get_user_nado_client", return_value=_RuntimeClient()
        ), patch.object(
            bot_runtime, "_dispatch_strategy", return_value={"success": True, "done": True, "stop_reason": "tp_hit"}
        ), patch.object(
            bot_runtime, "_save_state", side_effect=_save_state_stub
        ), patch.object(
            bot_runtime, "_finalize_session"
        ) as finalize_mock, patch.object(
            bot_runtime, "close_all_positions", return_value={"success": True}
        ) as close_mock, patch.object(
            bot_runtime, "_notify"
        ):
            result = asyncio.run(bot_runtime._run_cycle(telegram_id, network, state))

        self.assertEqual(result, (True, None))
        self.assertTrue(finalize_mock.called)
        self.assertTrue(close_mock.called)
        self.assertTrue(any(s.get("running") is False for s in saved_states))

    def test_vol_spot_idle_places_spot_buy_limit(self):
        state = {
            "product": "KBTC",
            "vol_market": "spot",
            "vol_direction": "long",
            "tp_pct": 1.0,
            "sl_pct": 1.0,
        }
        client = _VolClient(mid=100_000.0)
        with patch.object(volume_bot, "list_volume_spot_product_names", return_value=["KBTC"]), patch.object(
            volume_bot, "get_spot_product_id", return_value=42
        ), patch.object(
            volume_bot, "get_spot_metadata", return_value={"symbol": "KBTC", "id": 42}
        ), patch.object(
            volume_bot,
            "execute_spot_limit_order",
            return_value={"success": True, "digest": "s1", "price": 99900.0, "size": 0.001},
        ):
            result = volume_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client)
        self.assertTrue(result.get("success"))
        self.assertEqual(result.get("action"), "opened_limit_wait_fill")
        self.assertEqual(state.get("vol_phase"), "pending_fill")
        self.assertEqual(state.get("vol_market"), "spot")

    def test_vol_spot_pending_fill_detects_balance_fill(self):
        state = {
            "product": "KBTC",
            "vol_market": "spot",
            "vol_phase": "pending_fill",
            "vol_entry_digest": "d-open",
            "vol_entry_size": 1.0,
        }
        client = _VolClient(open_orders=[], spot_base_by_id={42: 1.0})
        with patch.object(volume_bot, "list_volume_spot_product_names", return_value=["KBTC"]), patch.object(
            volume_bot, "get_spot_product_id", return_value=42
        ), patch.object(volume_bot, "get_spot_metadata", return_value={"symbol": "KBTC"}):
            result = volume_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client)
        self.assertTrue(result.get("success"))
        self.assertEqual(result.get("action"), "entry_filled_wait_close")
        self.assertEqual(state.get("vol_phase"), "filled_wait_close")


if __name__ == "__main__":
    unittest.main()
