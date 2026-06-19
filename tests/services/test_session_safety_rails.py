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
    """Unrealized PnL + position come from the live VENUE position (baseline-
    adjusted) so the strategy SL agrees with Portfolio; realized/fees from the
    run's own tagged fills; volume from venue turnover."""

    def _snap(self, *, venue, metrics=None, mark, margin=100.0, baseline=None,
              turnover=None, client=None):
        metrics = metrics or {"fills": 0, "volume": 0.0, "fees": 0.0, "realized_pnl": 0.0}
        turnover = turnover or {"volume": 0.0, "fills": 0}
        sess = {"id": 1, "product_id": 2, "started_at": None, "stopped_at": None}
        if baseline:
            import json as _json
            sess["config_snapshot"] = _json.dumps(baseline)
        with patch.object(live_session, "_venue_position", return_value=venue), \
             patch("src.nadobro.models.database.get_session_live_metrics", return_value=metrics), \
             patch("src.nadobro.models.database.get_session_turnover", return_value=turnover), \
             patch("src.nadobro.models.database.count_open_orders_for_product", return_value=1):
            return live_session.get_live_session_snapshot(
                42, "mainnet", sess,
                state={"notional_usd": margin}, client=client, mark=mark,
            )

    def test_session_pnl_is_venue_upnl(self):
        # The screenshot SL scenario: venue position uPnL = -$10.38 on $100
        # margin, SL 10%. Session PnL must reflect the REAL -10.38% so the rail
        # fires (the bug: reconstructed fills read ~-0.9%).
        venue = {"size_signed": 0.08, "entry": 63266.0, "liq": 60953.0,
                 "leverage": 49.0, "margin_used": 100.0, "upnl": -10.38, "synced_ts": 9e18}
        snap = self._snap(venue=venue, mark=63135.0)
        self.assertAlmostEqual(snap["unrealized_pnl"], -10.38)
        self.assertAlmostEqual(snap["session_pnl"], -10.38)
        self.assertAlmostEqual(snap["session_pnl_pct"], -10.38)
        self.assertTrue(snap["has_position"])
        self.assertEqual(snap["position_side"], "long")
        self.assertAlmostEqual(snap["position_size"], 0.08)
        self.assertAlmostEqual(snap["position_value"], 0.08 * 63135.0)
        self.assertAlmostEqual(snap["liq_price"], 60953.0)

    def test_baseline_excludes_preexisting_position(self):
        # A position pre-existed at run start (5.0 BTC @ 60050). Venue now shows
        # 5.02 BTC with -$302 total uPnL. The run only added 0.02 — its PnL must
        # EXCLUDE the baseline's uPnL (no contamination from a manual position).
        mark = 60000.0
        baseline = {"baseline_size": 5.0, "baseline_entry": 60050.0}
        venue = {"size_signed": 5.02, "entry": 60048.0, "liq": 0.0,
                 "leverage": 0.0, "margin_used": 0.0, "upnl": -302.0, "synced_ts": 9e18}
        snap = self._snap(venue=venue, mark=mark, baseline=baseline)
        baseline_upnl = 5.0 * (mark - 60050.0)        # = -250
        self.assertAlmostEqual(snap["unrealized_pnl"], -302.0 - baseline_upnl)  # run-only
        self.assertGreater(snap["session_pnl"], -302.0)   # nowhere near the full -302
        self.assertAlmostEqual(snap["position_size"], 0.02)

    def test_no_position_reports_zero_unrealized(self):
        # Flat venue position -> unrealized 0, session_pnl == realized.
        venue = {"size_signed": 0.0, "entry": 0.0, "liq": 0.0, "leverage": 0.0,
                 "margin_used": 0.0, "upnl": 0.0, "synced_ts": 9e18}
        snap = self._snap(venue=venue, mark=64000.0,
                          metrics={"fills": 8, "volume": 0.0, "fees": 2.0, "realized_pnl": 5.0})
        self.assertAlmostEqual(snap["unrealized_pnl"], 0.0)
        self.assertAlmostEqual(snap["session_pnl"], 5.0)
        self.assertFalse(snap["has_position"])
        self.assertEqual(snap["position_side"], "")

    def test_conservation_pnl_is_realized_plus_unrealized(self):
        venue = {"size_signed": 0.02, "entry": 63000.0, "liq": 0.0, "leverage": 0.0,
                 "margin_used": 0.0, "upnl": 7.5, "synced_ts": 9e18}
        snap = self._snap(venue=venue, mark=63375.0,
                          metrics={"fills": 6, "volume": 0.0, "fees": 1.0, "realized_pnl": 3.5})
        self.assertAlmostEqual(snap["session_pnl"],
                               snap["realized_pnl"] + snap["unrealized_pnl"])

    def test_volume_uses_venue_turnover(self):
        # Session volume = real turnover on the product (matches Nado), not the
        # under-counted tagged-fill sum.
        venue = {"size_signed": 0.08, "entry": 63266.0, "liq": 0.0, "leverage": 0.0,
                 "margin_used": 0.0, "upnl": -10.0, "synced_ts": 9e18}
        snap = self._snap(venue=venue, mark=63135.0,
                          metrics={"fills": 4, "volume": 2330.0, "fees": 0.5, "realized_pnl": 0.0},
                          turnover={"volume": 6100.0, "fills": 40})
        self.assertAlmostEqual(snap["volume"], 6100.0)
        self.assertEqual(snap["fills"], 40)


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
