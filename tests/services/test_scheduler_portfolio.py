"""Scheduler + portfolio sync stability tests for the workflow redesign."""
from __future__ import annotations

import asyncio
import time
import unittest
from unittest.mock import patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.services import nado_sync, scheduler


class SchedulerPortfolioTests(unittest.IsolatedAsyncioTestCase):
    def test_resolve_market_snapshot_lock_returns_usable_lock(self):
        lock_a = scheduler._resolve_market_snapshot_lock()
        lock_b = scheduler._resolve_market_snapshot_lock()
        self.assertIsInstance(lock_a, asyncio.Lock)
        self.assertIs(lock_a, lock_b)

    async def test_get_market_snapshot_serializes_concurrent_fetches(self):
        """Concurrent cache misses must not double-fetch market prices."""
        scheduler._last_market_snapshot = {"ts": 0.0, "prices": {"BTC": 1.0}}
        calls = {"n": 0}

        class _Client:
            def get_all_market_prices(self):
                calls["n"] += 1
                return {"BTC": 100.0}

        scheduler._check_client = _Client()
        await asyncio.gather(
            scheduler._get_market_snapshot(force_refresh=False),
            scheduler._get_market_snapshot(force_refresh=False),
        )
        self.assertEqual(calls["n"], 1)

    async def test_sync_user_respects_max_age_ms(self):
        nado_sync.clear_cache()
        key = (42, "mainnet")
        nado_sync._snapshot_cache[key] = {
            "user_id": 42,
            "network": "mainnet",
            "monotonic_ts": time.time() - 3.0,
            "positions": [],
            "stats": {},
        }
        calls = {"n": 0}

        def _client(*args, **kwargs):
            calls["n"] += 1
            raise RuntimeError("venue down")

        user = type("U", (), {"network_mode": type("NM", (), {"value": "mainnet"})()})()
        with patch.object(nado_sync, "get_user", return_value=user), patch.object(
            nado_sync, "get_user_nado_client", side_effect=_client
        ), patch.object(nado_sync, "_write_sync_log_error"):
            result = await nado_sync.sync_user(42, network="mainnet", max_age_ms=2000)

        self.assertEqual(calls["n"], 1)
        self.assertTrue(result.get("stale"))
        nado_sync.clear_cache()


class SessionRollupTests(unittest.TestCase):
    def test_rollup_session_from_trades_overwrites_counters(self):
        from src.nadobro.models.database import rollup_session_from_trades

        row = {
            "filled": 3,
            "cancelled": 1,
            "realized_pnl": 42.5,
            "fees": 1.25,
            "volume": 5000.0,
            "funding": -0.5,
            "wins": 2,
            "losses": 1,
        }
        with patch("src.nadobro.models.database.query_one", return_value=row), patch(
            "src.nadobro.models.database.update_strategy_session"
        ) as update_mock:
            totals = rollup_session_from_trades(99, "mainnet")

        self.assertEqual(totals["realized_pnl"], 42.5)
        self.assertEqual(totals["total_volume_usd"], 5000.0)
        self.assertEqual(totals["win_count"], 2)
        self.assertEqual(totals["loss_count"], 1)
        update_mock.assert_called_once()
        self.assertEqual(update_mock.call_args[0][0], 99)
