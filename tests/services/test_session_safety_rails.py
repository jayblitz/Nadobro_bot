"""Regression coverage for the live-PnL session safety rails + snapshot.

These guard the bug where a 1%-of-margin stop-loss rode all the way to a ~$32
loss: the rail in ``bot_runtime._run_cycle`` was gated on engine result actions
(``grid_stop_loss_hit`` etc.) that ``run_engine_cycle`` never emits, so it never
fired. The rail now reads live Nado session PnL (realized + unrealized) measured
as a percentage of the configured margin.
"""

import unittest
from unittest.mock import AsyncMock, patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.services import bot_runtime, engine_runtime, live_session, mm_dashboard


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
        self._engine_stop = AsyncMock()
        with patch.object(bot_runtime, "run_blocking", _fake_run_blocking), \
             patch("src.nadobro.models.database.get_active_strategy_session", return_value=sess), \
             patch("src.nadobro.services.live_session.get_live_session_snapshot", return_value=snap), \
             patch.object(engine_runtime.RUNTIME, "stop", new=self._engine_stop), \
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
        # INVARIANT (Cleanup): the engine controller is stopped (resting orders
        # cancelled via _stop_out) BEFORE the position is flattened.
        self._engine_stop.assert_awaited_once_with(42, "mainnet", "dgrid")

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
    def _snap(self, metrics, *, mark, client=None, margin=100.0, sess=None):
        sess = sess or {"id": 1, "product_id": 2, "started_at": None, "stopped_at": None}
        with patch("src.nadobro.models.database.get_session_live_metrics",
                   return_value=metrics), \
             patch("src.nadobro.models.database.count_open_orders_for_product", return_value=1):
            return live_session.get_live_session_snapshot(
                42, "mainnet", sess,
                state={"notional_usd": margin}, client=client, mark=mark,
            )

    def test_session_pnl_includes_unrealized(self):
        # The -$32 scenario, now sourced from the session's OWN net base marked
        # to the live mid: net_base*mark + signed_cash = realized + unrealized.
        # net_base=0.05 @ mark=100000 -> +5000 of base value; signed_cash chosen
        # so gross = -32, with realized -2 -> unrealized -30.
        metrics = {
            "fills": 4, "volume": 1000.0, "fees": 0.5, "realized_pnl": -2.0,
            "net_base": 0.05, "signed_cash": -5032.0,
        }
        snap = self._snap(metrics, mark=100000.0)
        self.assertAlmostEqual(snap["unrealized_pnl"], -30.0)
        self.assertAlmostEqual(snap["realized_pnl"], -2.0)
        self.assertAlmostEqual(snap["session_pnl"], -32.0)
        self.assertAlmostEqual(snap["session_pnl_pct"], -32.0)
        self.assertTrue(snap["has_position"])
        self.assertEqual(snap["position_side"], "long")

    def test_isolation_account_position_does_not_contaminate(self):
        # INVARIANT (Isolation): the session only bought 0.0016 BTC (real PnL
        # ~ -$0.13), but the ACCOUNT holds a -$302 position on the same product.
        # The snapshot must reflect ONLY the session's own fills, never the
        # account aggregate (the false-SL bug, session #40).
        class _BigPosClient:
            def get_all_positions(self):
                return [{"product_id": 2, "amount": 5.0, "side": "LONG",
                         "signed_amount": 5.0, "unrealized_pnl": -302.0,
                         "price": 65000.0, "liquidation_price": 61000.0}]
            def get_market_price(self, pid):
                return {"mid": 64990.0}
            def get_open_orders(self, pid):
                return []
        # session bought 0.0016 @ ~65000 -> signed_cash = -(0.0016*65000) = -104.0
        metrics = {
            "fills": 1, "volume": 104.0, "fees": 0.05, "realized_pnl": 0.0,
            "net_base": 0.0016, "signed_cash": -104.0,
        }
        snap = self._snap(metrics, mark=64990.0, client=_BigPosClient())
        # session uPnL = 0.0016*(64990-65000) = -0.016, NOT -302.
        self.assertAlmostEqual(snap["session_pnl"], 0.0016 * 64990.0 - 104.0, places=6)
        self.assertGreater(snap["session_pnl"], -1.0)
        self.assertNotAlmostEqual(snap["session_pnl"], -302.0, places=1)
        self.assertAlmostEqual(snap["position_size"], 0.0016)

    def test_conservation_gross_equals_realized_plus_unrealized(self):
        # INVARIANT (Conservation): session_pnl == realized + unrealized (funding 0).
        metrics = {
            "fills": 6, "volume": 2000.0, "fees": 1.0, "realized_pnl": 3.5,
            "net_base": 0.02, "signed_cash": -1290.0,
        }
        snap = self._snap(metrics, mark=64000.0)
        gross = snap["realized_pnl"] + snap["unrealized_pnl"]
        self.assertAlmostEqual(snap["session_pnl"], gross)
        self.assertAlmostEqual(gross, -1290.0 + 0.02 * 64000.0)

    def test_no_mark_open_leg_reports_zero_upnl_not_phantom_loss(self):
        # INVARIANT (No false SL): with an OPEN long but NO live mark available
        # (client None, no mark passed), the open leg must read uPnL=0 (realized
        # basis), NOT signed_cash (-$104) which would trip a phantom SL.
        metrics = {
            "fills": 1, "volume": 104.0, "fees": 0.05, "realized_pnl": 0.0,
            "net_base": 0.0016, "signed_cash": -104.0,
        }
        snap = self._snap(metrics, mark=0.0, client=None)
        self.assertAlmostEqual(snap["unrealized_pnl"], 0.0)
        self.assertAlmostEqual(snap["session_pnl"], 0.0)  # == realized (0)
        self.assertGreater(snap["session_pnl_pct"], -1.0)

    def test_conservation_closed_session_has_zero_unrealized(self):
        # When fully closed (net_base==0), gross == realized, unrealized == 0.
        metrics = {
            "fills": 8, "volume": 4000.0, "fees": 2.0, "realized_pnl": 5.0,
            "net_base": 0.0, "signed_cash": 5.0,
        }
        snap = self._snap(metrics, mark=64000.0)
        self.assertAlmostEqual(snap["unrealized_pnl"], 0.0)
        self.assertAlmostEqual(snap["session_pnl"], 5.0)
        self.assertFalse(snap["has_position"])
        self.assertEqual(snap["position_side"], "")


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
        # PnL leads with the per-run realized+unrealized session PnL, then a
        # realized/unrealized breakdown.
        self.assertIn("PnL (realized+unrealized): $-32.00", text)
        self.assertIn("-32.00%", text)
        self.assertIn("realized $-2.00 | unrealized $-30.00", text)
        self.assertIn("Position: LONG", text)


if __name__ == "__main__":
    unittest.main()
