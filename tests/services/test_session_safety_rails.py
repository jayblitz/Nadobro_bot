"""Regression coverage for the live-PnL session safety rails + snapshot.

These guard the bug where a 1%-of-margin stop-loss rode all the way to a ~$32
loss: the rail in ``bot_runtime._run_cycle`` was gated on engine result actions
(``grid_stop_loss_hit`` etc.) that ``run_engine_cycle`` never emits, so it never
fired. The rail now reads live Nado session PnL (realized + unrealized) measured
as a percentage of the configured margin.
"""

import time
import unittest
from unittest.mock import AsyncMock, patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.services import bot_runtime, live_session, mm_dashboard


async def _fake_run_blocking(fn, *args, **kwargs):
    return fn(*args, **kwargs)


class SessionPnlRailTests(unittest.IsolatedAsyncioTestCase):
    async def _run_rail(self, snap, *, sl=1.0, tp=2.0):
        state = {"sl_pct": sl, "tp_pct": tp, "strategy": "dgrid"}
        closed = {}

        async def close_coro():
            closed["called"] = True
            return {"success": True}

        sess = {"id": 1, "product_id": 2, "started_at": None, "stopped_at": None}
        with patch.object(bot_runtime, "run_blocking", _fake_run_blocking), \
             patch("src.nadobro.models.database.get_active_strategy_session", return_value=sess), \
             patch("src.nadobro.services.live_session.get_live_session_snapshot", return_value=snap), \
             patch.object(bot_runtime, "_finalize_session") as fin, \
             patch.object(bot_runtime, "_save_state"), \
             patch.object(bot_runtime, "_notify", new=AsyncMock()), \
             patch.object(bot_runtime, "_strategy_display_name", return_value="DGRID"):
            res = await bot_runtime._evaluate_session_pnl_rail(
                42, "mainnet", state, "dgrid", "BTC",
                client=None, close_coro=close_coro,
            )
        return res, closed, state, fin

    async def test_sl_fires_on_unrealized_drawdown(self):
        # The screenshot scenario: -$32 on $100 margin = -32% of margin, SL=1%.
        snap = {"session_pnl": -32.0, "session_pnl_pct": -32.0, "margin": 100.0}
        res, closed, state, fin = await self._run_rail(snap, sl=1.0)
        self.assertEqual(res, (True, None))
        self.assertTrue(closed.get("called"))
        self.assertFalse(state["running"])
        fin.assert_called_once()
        self.assertEqual(fin.call_args.kwargs.get("stop_reason"), "sl_hit")

    async def test_tp_fires_when_pct_above_target(self):
        snap = {"session_pnl": 2.5, "session_pnl_pct": 2.5, "margin": 100.0}
        res, closed, state, fin = await self._run_rail(snap, tp=2.0)
        self.assertEqual(res, (True, None))
        self.assertTrue(closed.get("called"))
        self.assertEqual(fin.call_args.kwargs.get("stop_reason"), "tp_hit")
        self.assertIsNone(state["last_error"])

    async def test_no_stop_within_band(self):
        snap = {"session_pnl": -0.5, "session_pnl_pct": -0.5, "margin": 100.0}
        res, closed, _state, fin = await self._run_rail(snap, sl=1.0, tp=2.0)
        self.assertIsNone(res)
        self.assertFalse(closed.get("called"))
        fin.assert_not_called()

    async def test_no_basis_when_margin_zero(self):
        snap = {"session_pnl": -50.0, "session_pnl_pct": 0.0, "margin": 0.0}
        res, closed, _state, fin = await self._run_rail(snap, sl=1.0)
        self.assertIsNone(res)
        self.assertFalse(closed.get("called"))


class LiveSnapshotMathTests(unittest.TestCase):
    def test_session_pnl_includes_unrealized(self):
        rows = [{
            "side": "long", "size": 0.0527, "avg_entry_price": 65558.0,
            "est_liq_price": 61765.0, "est_pnl": -30.0, "margin_used": 100.0,
            "leverage": 16.7, "isolated": False, "synced_ts": time.time(),
        }]
        with patch("src.nadobro.models.database.get_session_live_metrics",
                   return_value={"fills": 4, "volume": 1000.0, "fees": 0.5, "realized_pnl": -2.0}), \
             patch("src.nadobro.models.database.get_open_position_rows_for_product",
                   return_value=rows), \
             patch("src.nadobro.models.database.count_open_orders_for_product", return_value=1):
            snap = live_session.get_live_session_snapshot(
                42, "mainnet",
                {"id": 1, "product_id": 2, "started_at": None, "stopped_at": None},
                state={"notional_usd": 100.0}, client=None,
            )
        self.assertAlmostEqual(snap["unrealized_pnl"], -30.0)
        self.assertAlmostEqual(snap["realized_pnl"], -2.0)
        # realized + unrealized - funding = -2 + -30 - 0
        self.assertAlmostEqual(snap["session_pnl"], -32.0)
        self.assertAlmostEqual(snap["session_pnl_pct"], -32.0)
        self.assertTrue(snap["has_position"])
        self.assertEqual(snap["position_side"], "long")


class StatusRenderTests(unittest.TestCase):
    def test_status_lines_show_upnl_and_session_pnl(self):
        snap = {
            "unrealized_pnl": -30.0, "session_pnl": -32.0, "session_pnl_pct": -32.0,
            "margin": 100.0, "realized_pnl": -2.0, "volume": 1000.0, "fees": 0.5,
            "fills": 4, "open_orders": 1, "has_position": True, "position_size": 0.0527,
            "position_side": "long", "entry_price": 65558.0, "liq_price": 61765.0,
        }
        s = mm_dashboard.build_status_snapshot(
            state={"running": True}, strategy_id="dgrid", network="mainnet",
            product="BTC", open_orders_count=0, live_snapshot=snap,
        )
        text = "\n".join(mm_dashboard.render_status_lines(s))
        self.assertIn("Unrealized PnL: $-30.00", text)
        self.assertIn("Session PnL (realized+unrealized): $-32.00", text)
        self.assertIn("-32.00%", text)
        self.assertIn("Position: LONG", text)


if __name__ == "__main__":
    unittest.main()
