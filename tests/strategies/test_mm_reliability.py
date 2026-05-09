"""Phase 4 reliability hardening: integration tests around mm_bot.run_cycle.

Locks in:
  - First cycle of a new strategy_session_id stamps mm_resume_reconciled_at
    and the executed/tracked counts so /mm_status can show the reconcile pass.
  - Subsequent cycles in the same session do NOT re-stamp.
  - Post-only retry exhaustion appends a skipped-level entry to state and the
    cycle result rather than dropping silently.
  - Transient 429s on get_market_price / get_open_orders are retried (mm_bot
    does not abort the cycle) and surfaced in state["mm_*_retries"].
  - Dashboard snapshot exposes skipped + reconcile + retry fields.
"""
import unittest
from unittest.mock import patch

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from _stubs import install_test_stubs  # noqa: E402

install_test_stubs()

from src.nadobro.services import mm_dashboard  # noqa: E402
from src.nadobro.strategies import mm_bot  # noqa: E402


class _ReliabilityClient:
    def __init__(self, mid=10000.0, balance=100_000.0, market_price_side_effects=None,
                 open_orders_side_effects=None, post_only_failure_count=0):
        self._mid = mid
        self._balance = float(balance)
        # Sequence of side effects for retry tests.
        self._mp_side_effects = list(market_price_side_effects or [])
        self._oo_side_effects = list(open_orders_side_effects or [])
        # Force the first N execute_limit_order calls (and their retries) to
        # fail with the post-only "crosses the book" error.
        self._post_only_failure_count = int(post_only_failure_count)
        self.cancelled = []

    def get_market_price(self, _product_id):
        if self._mp_side_effects:
            effect = self._mp_side_effects.pop(0)
            if isinstance(effect, Exception):
                raise effect
            return effect
        return {"mid": self._mid, "bid": self._mid - 1.0, "ask": self._mid + 1.0}

    def get_open_orders(self, _product_id):
        if self._oo_side_effects:
            effect = self._oo_side_effects.pop(0)
            if isinstance(effect, Exception):
                raise effect
            return list(effect)
        return []

    def get_all_positions(self):
        return []

    def get_balance(self):
        return {"exists": True, "balances": {0: self._balance}, "equity": self._balance}

    def cancel_order(self, _product_id, digest):
        self.cancelled.append(digest)
        return {"success": True}


def _patched_run(state, client, post_only_failure=False):
    """Run mm_bot.run_cycle with the standard catalog/leverage patches."""
    success_response = {"success": True, "digest": "d1"}
    fail_response = {"success": False, "error": "POST-ONLY order crosses the book"}

    def _exec_side_effect(*args, **kwargs):
        if post_only_failure:
            return fail_response
        return success_response

    with patch.object(mm_bot, "get_product_id", return_value=2), patch.object(
        mm_bot, "get_product_max_leverage", return_value=10.0
    ), patch.object(mm_bot, "execute_limit_order", side_effect=_exec_side_effect), patch.object(
        mm_bot, "_reprice_post_only_quote", side_effect=lambda *a, **kw: float(state["product_mid"]) + 0.5 * (kw.get("attempt", 0) + 1)
    ):
        return mm_bot.run_cycle(
            telegram_id=1, network="mainnet", state=state, client=client,
            mid=state["product_mid"], open_orders=[],
        )


class ResumeReconcileTests(unittest.TestCase):
    def _seed_state(self, session_id="session-123"):
        return {
            "product": "BTC",
            "product_mid": 10000.0,
            "strategy": "mid",
            "spread_bp": 5.0,
            "levels": 1,
            "notional_usd": 1000.0,
            "min_order_notional_usd": 10.0,
            "directional_bias": 0.0,
            "reference_mode": "mid",
            "inventory_soft_limit_usd": 100_000.0,
            "strategy_session_id": session_id,
            "mm_initial_equity": 5000.0,
            # Pre-existing fills imply this is a resume scenario.
            "grid_buy_fills": [{"price": 9990.0, "size": 0.001, "ts": 1700000000}],
            "grid_sell_fills": [{"price": 10010.0, "size": 0.001, "ts": 1700000000}],
        }

    def test_first_cycle_stamps_resume_marker(self):
        state = self._seed_state(session_id="session-A")
        client = _ReliabilityClient(mid=10000.0)
        _patched_run(state, client)
        self.assertIsNotNone(state.get("mm_resume_reconciled_at"))
        self.assertEqual(state.get("mm_resume_reconcile_session_id"), "session-A")
        # tracked count was 0 at the start of the cycle (no live tracked quotes)
        # but executed_count exists as a key.
        self.assertIn("mm_resume_executed_count", state)
        self.assertIn("mm_resume_tracked_count", state)

    def test_second_cycle_same_session_does_not_re_stamp(self):
        state = self._seed_state(session_id="session-B")
        client = _ReliabilityClient(mid=10000.0)
        _patched_run(state, client)
        first_ts = state["mm_resume_reconciled_at"]
        # Run again — same session_id, marker should NOT advance.
        _patched_run(state, client)
        self.assertEqual(state["mm_resume_reconciled_at"], first_ts)

    def test_new_session_stamps_fresh_marker(self):
        state = self._seed_state(session_id="session-old")
        client = _ReliabilityClient(mid=10000.0)
        _patched_run(state, client)
        first_ts = state["mm_resume_reconciled_at"]
        # Simulate a new strategy_session — restart with a different id.
        state["strategy_session_id"] = "session-new"
        _patched_run(state, client)
        # Different session_id ⇒ new reconcile.
        self.assertEqual(state["mm_resume_reconcile_session_id"], "session-new")
        # Timestamp may equal (clock resolution) but key must reflect new session.
        self.assertGreaterEqual(state["mm_resume_reconciled_at"], first_ts)

    def test_no_marker_when_no_persisted_state(self):
        # A genuinely fresh session (no fills, no tracked quotes) does not
        # warrant a "resume" marker — there's nothing to reconcile.
        state = self._seed_state(session_id="fresh")
        state.pop("grid_buy_fills", None)
        state.pop("grid_sell_fills", None)
        state.pop("mm_tracked_quotes", None)
        client = _ReliabilityClient(mid=10000.0)
        _patched_run(state, client)
        self.assertIsNone(state.get("mm_resume_reconciled_at"))

    def test_marker_re_stamps_after_simulated_kill_restart(self):
        """F1 regression guard.

        After a kill+restart, the persisted state has BOTH ``strategy_session_id``
        and ``mm_resume_reconcile_session_id`` set to the same value (the
        previous process stamped it then died). A correct implementation must
        still re-stamp on the first cycle of the *new* process. We simulate the
        new process by clearing the module-level ``_PROCESS_RECONCILED_SESSIONS``
        set.
        """
        # Pre-existing state: session was previously reconciled.
        state = self._seed_state(session_id="S-survived-restart")
        state["mm_resume_reconcile_session_id"] = "S-survived-restart"
        state["mm_resume_reconciled_at"] = 1_700_000_000.0
        # Simulate a fresh Python process — the in-memory set is empty.
        mm_bot._PROCESS_RECONCILED_SESSIONS.clear()
        client = _ReliabilityClient(mid=10000.0)
        _patched_run(state, client)
        # Marker re-stamps despite matching persisted session_id.
        self.assertNotEqual(state["mm_resume_reconciled_at"], 1_700_000_000.0)
        self.assertEqual(state["mm_resume_reconcile_session_id"], "S-survived-restart")
        # And the in-process set now contains it, so subsequent cycles in this
        # process won't re-stamp.
        self.assertIn("S-survived-restart", mm_bot._PROCESS_RECONCILED_SESSIONS)


class SkippedLevelTests(unittest.TestCase):
    def test_post_only_retry_exhaustion_records_skipped_level(self):
        state = {
            "product": "BTC",
            "product_mid": 10000.0,
            "strategy": "mid",
            "spread_bp": 5.0,
            "levels": 1,
            "notional_usd": 1000.0,
            "min_order_notional_usd": 10.0,
            "directional_bias": 0.0,
            "reference_mode": "mid",
            "inventory_soft_limit_usd": 100_000.0,
            "strategy_session_id": "session-skip",
        }
        client = _ReliabilityClient(mid=10000.0)
        result = _patched_run(state, client, post_only_failure=True)
        skips = state.get("mm_skipped_levels") or []
        self.assertGreater(len(skips), 0)
        s = skips[0]
        self.assertEqual(s["reason"], "post_only_retries_exhausted")
        self.assertIn(s["side"], ("BUY", "SELL"))
        self.assertEqual(s["attempts"], mm_bot.POST_ONLY_REPRICE_MAX_RETRIES)
        # Cycle result also surfaces the count.
        self.assertEqual(result.get("skipped_levels_count"), len(skips))

    def test_no_skipped_levels_on_clean_cycle(self):
        state = {
            "product": "BTC",
            "product_mid": 10000.0,
            "strategy": "mid",
            "spread_bp": 5.0,
            "levels": 1,
            "notional_usd": 1000.0,
            "min_order_notional_usd": 10.0,
            "directional_bias": 0.0,
            "reference_mode": "mid",
            "inventory_soft_limit_usd": 100_000.0,
            "strategy_session_id": "session-clean",
        }
        client = _ReliabilityClient(mid=10000.0)
        result = _patched_run(state, client, post_only_failure=False)
        self.assertEqual(state.get("mm_skipped_levels"), [])
        self.assertEqual(result.get("skipped_levels_count"), 0)


class RateLimitedGatewayTests(unittest.TestCase):
    def test_transient_market_price_429_does_not_abort_cycle(self):
        state = {
            "product": "BTC",
            "product_mid": 10000.0,
            "strategy": "mid",
            "spread_bp": 5.0,
            "levels": 1,
            "notional_usd": 1000.0,
            "min_order_notional_usd": 10.0,
            "directional_bias": 0.0,
            "reference_mode": "mid",
            "inventory_soft_limit_usd": 100_000.0,
            "strategy_session_id": "session-429",
        }
        # First call raises 429; second call returns the real mid.
        client = _ReliabilityClient(
            mid=10000.0,
            market_price_side_effects=[
                Exception("HTTP 429 Too Many Requests"),
                {"bid": 9999.0, "ask": 10001.0, "mid": 10000.0},
            ],
        )
        # Patch sleep to keep the test fast.
        with patch("src.nadobro.services.rate_limit.time.sleep"):
            result = _patched_run(state, client)
        self.assertTrue(result.get("success"))
        retries = state.get("mm_market_price_retries") or []
        self.assertGreater(len(retries), 0)
        self.assertIn("429", retries[0])

    def test_persistent_market_price_failure_aborts_cleanly(self):
        state = {
            "product": "BTC",
            "product_mid": 10000.0,
            "strategy": "mid",
            "spread_bp": 5.0,
            "levels": 1,
            "notional_usd": 1000.0,
            "min_order_notional_usd": 10.0,
            "strategy_session_id": "session-persist",
        }
        # All attempts raise 429 — cycle should return a structured failure
        # rather than blowing up the worker.
        client = _ReliabilityClient(
            mid=10000.0,
            market_price_side_effects=[
                Exception("HTTP 429 persistent"),
                Exception("HTTP 429 persistent"),
                Exception("HTTP 429 persistent"),
            ],
        )
        with patch("src.nadobro.services.rate_limit.time.sleep"):
            result = _patched_run(state, client)
        self.assertFalse(result.get("success"))
        self.assertIn("market_price unavailable after retries", result.get("error", ""))


class DashboardReliabilityFieldsTests(unittest.TestCase):
    def test_snapshot_includes_phase4_fields(self):
        state = {
            "running": True,
            "leverage": 10.0,
            "spread_bp": 5.0,
            "reference_price": 10000.0,
            "inventory_soft_limit_usd": 60.0,
            "mm_skipped_levels": [
                {"level": 2, "side": "BUY", "reason": "post_only_retries_exhausted",
                 "intended_price": 100.0, "last_attempted_price": 99.0, "attempts": 3, "error": "x"},
            ],
            "mm_resume_reconciled_at": 1700000000.0,
            "mm_resume_executed_count": 2,
            "mm_resume_tracked_count": 5,
            "mm_market_price_retries": ["get_market_price: HTTP 429"],
        }
        snap = mm_dashboard.build_status_snapshot(
            state=state, strategy_id="mid", network="mainnet", product="BTC", open_orders_count=0
        )
        self.assertEqual(snap["skipped_levels_count"], 1)
        self.assertEqual(snap["resume_executed_count"], 2)
        self.assertEqual(snap["resume_tracked_count"], 5)
        self.assertEqual(len(snap["market_price_retries"]), 1)

    def test_render_lines_surface_skipped_and_reconcile(self):
        snap = {
            "strategy_id": "mid",
            "product": "BTC",
            "network": "mainnet",
            "running": True,
            "is_paused": False,
            "leverage": 10.0,
            "leverage_mode": "MAX",
            "session_done_usd": 0.0,
            "session_target_usd": 0.0,
            "session_progress_pct": 0.0,
            "cumulative_pnl_usd": 0.0,
            "drawdown_pct": 0.0,
            "last_cycle_pnl_usd": 0.0,
            "open_orders_count": 0,
            "tracked_quotes_count": 0,
            "fill_count": 0,
            "fill_rate": 0.0,
            "spread_bp": 5.0,
            "reference_price": 10000.0,
            "inv_usd": 0.0,
            "inv_soft_limit_usd": 0.0,
            "skipped_levels_count": 2,
            "skipped_levels": [
                {"level": 1, "side": "BUY", "reason": "post_only_retries_exhausted"},
                {"level": 1, "side": "SELL", "reason": "post_only_retries_exhausted"},
            ],
            "resume_reconciled_at": 1700000000.0,
            "resume_executed_count": 0,
            "resume_tracked_count": 3,
            "market_price_retries": ["get_market_price: 429 throttle"],
            "open_orders_retries": [],
        }
        lines = mm_dashboard.render_status_lines(snap)
        joined = "\n".join(lines)
        self.assertIn("Skipped this cycle", joined)
        self.assertIn("Resume reconcile", joined)
        self.assertIn("Gateway retries", joined)


if __name__ == "__main__":
    unittest.main()
