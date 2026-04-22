"""Tests that lock in the volume_bot perp-recycle and spot fixes.

Covers the exact bugs the user reported:

    "Spot Volume not working."
    "Perp volume is supposed to fill, close, and re-enter. This way, the same
     margin is used repeatedly. But currently, it compiles and uses up the
     margin."

These reproduce the margin-stacking symptom and verify the TTL-based
cancel-and-repost + IOC escalation pathway releases margin so the next cycle
can re-enter.
"""
import time
import unittest
from unittest.mock import patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.strategies import volume_bot


class _VolClient:
    """Minimal fake Nado client for volume_bot.run_cycle."""

    def __init__(
        self,
        mid=100.0,
        open_orders=None,
        positions=None,
        network="mainnet",
        balance=1000.0,
        spot_base_by_id=None,
        cancel_result=None,
    ):
        self._mid = mid
        self._open_orders = open_orders or []
        self._positions = positions or []
        self._balance = float(balance)
        self._spot_base = dict(spot_base_by_id or {})
        self.network = network
        self.cancelled = []
        self._cancel_result = cancel_result or {"success": True}

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

    def cancel_order(self, product_id, digest):
        self.cancelled.append((product_id, digest))
        self._open_orders = [o for o in self._open_orders if str(o.get("digest")) != str(digest)]
        return dict(self._cancel_result)


class PerpCloseTtlTests(unittest.TestCase):
    """Locks in the perp `pending_close_fill` TTL / escalation ladder."""

    def _state_with_stale_close(self, entry_offset_s: float, close_offset_s: float):
        now = time.time()
        return {
            "product": "BTC",
            "vol_phase": "pending_close_fill",
            "vol_direction": "long",
            "vol_entry_fill_ts": now - entry_offset_s,
            "vol_entry_fill_price": 100.0,
            "vol_entry_size": 1.0,
            "vol_close_digest": "d-stale-close",
            "vol_close_size": 1.0,
            "vol_close_posted_ts": now - close_offset_s,
            "tp_pct": 0.0,
            "sl_pct": 0.0,
            "session_realized_pnl_usd": 0.0,
        }

    def test_stale_close_triggers_cancel_for_repost_after_threshold(self):
        # Close posted 60s ago (> CLOSE_REPOST_AFTER_SECONDS=45) but total
        # entry age only 90s (< CLOSE_ESCALATE_AFTER_SECONDS=180) — should
        # cancel and transition back to `filled_wait_close` for a fresh quote.
        state = self._state_with_stale_close(entry_offset_s=90.0, close_offset_s=60.0)
        client = _VolClient(
            mid=100.0,
            open_orders=[{"digest": "d-stale-close", "price": 100.0}],
            positions=[{"product_id": 2, "amount": 1.0, "side": "LONG"}],
        )
        with patch.object(volume_bot, "get_product_id", return_value=2):
            result = volume_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client)
        self.assertTrue(result["success"])
        self.assertEqual(result["action"], "close_stale_cancel_for_repost")
        self.assertEqual(state["vol_phase"], "filled_wait_close")
        self.assertIsNone(state["vol_close_digest"])
        self.assertEqual(state["vol_close_posted_ts"], 0.0)
        # The stuck close must have been cancelled.
        self.assertIn((2, "d-stale-close"), client.cancelled)

    def test_super_stale_close_escalates_to_market_force_close(self):
        # Entry placed 300s ago (> CLOSE_ESCALATE_AFTER_SECONDS=180). Must
        # cancel the resting post-only and fire a reduce-only market order to
        # release margin. Before the fix, this path didn't exist and margin
        # stayed locked indefinitely.
        state = self._state_with_stale_close(entry_offset_s=300.0, close_offset_s=200.0)
        client = _VolClient(
            mid=100.0,
            open_orders=[{"digest": "d-stale-close", "price": 100.0}],
            positions=[{"product_id": 2, "amount": 1.0, "side": "LONG"}],
        )
        with patch.object(volume_bot, "get_product_id", return_value=2), patch.object(
            volume_bot,
            "execute_market_order",
            return_value={"success": True, "digest": "d-force"},
        ) as market_mock:
            result = volume_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client)
        self.assertTrue(result["success"])
        self.assertEqual(result["action"], "close_escalated_force_close")
        # Force-close call must be reduce_only=True and opposite side (SHORT because pos is LONG).
        self.assertTrue(market_mock.called)
        kwargs = market_mock.call_args.kwargs
        self.assertTrue(kwargs.get("reduce_only"))
        self.assertFalse(kwargs.get("is_long"))
        # The stale post-only was cancelled before the escalate.
        self.assertIn((2, "d-stale-close"), client.cancelled)
        # State was updated to reflect new escalate digest.
        self.assertEqual(state["vol_close_digest"], "d-force")
        self.assertGreater(state["vol_close_posted_ts"], 0.0)

    def test_escalate_is_rate_limited_by_force_close_cooldown(self):
        state = self._state_with_stale_close(entry_offset_s=300.0, close_offset_s=200.0)
        state["vol_last_force_close_attempt_ts"] = time.time() - 5.0
        client = _VolClient(
            mid=100.0,
            open_orders=[{"digest": "d-stale-close", "price": 100.0}],
            positions=[{"product_id": 2, "amount": 1.0, "side": "LONG"}],
        )
        with patch.object(volume_bot, "get_product_id", return_value=2), patch.object(
            volume_bot, "execute_market_order"
        ) as market_mock:
            result = volume_bot.run_cycle(
                telegram_id=1, network="mainnet", state=state, client=client
            )
        self.assertTrue(result["success"])
        self.assertEqual(result["action"], "waiting_force_close_cooldown")
        self.assertFalse(market_mock.called)

    def test_close_posted_ts_is_set_when_close_is_first_placed(self):
        # Fresh `filled_wait_close` transitioning to `pending_close_fill` —
        # we need the TTL clock to start now, not stay at 0.
        state = {
            "product": "BTC",
            "vol_phase": "filled_wait_close",
            "vol_direction": "long",
            "vol_entry_fill_ts": time.time() - 61.0,
            "vol_entry_fill_price": 100.0,
            "vol_entry_size": 1.0,
        }
        client = _VolClient(
            mid=100.0,
            positions=[{"product_id": 2, "amount": 1.0, "side": "LONG"}],
        )
        with patch.object(volume_bot, "get_product_id", return_value=2), patch.object(
            volume_bot,
            "execute_limit_order",
            return_value={"success": True, "digest": "d-new-close", "price": 100.0},
        ):
            result = volume_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client)
        self.assertTrue(result["success"])
        self.assertEqual(state["vol_phase"], "pending_close_fill")
        # New field: ts of the close post, used by the TTL ladder above.
        self.assertGreater(state.get("vol_close_posted_ts", 0.0), 0.0)

    def test_stale_close_digest_is_cancelled_before_reposting(self):
        # If a prior close_digest lingered in state, posting a new close on
        # the next `filled_wait_close` tick must cancel the stale one first
        # so two reduce-only quotes aren't resting at the same time.
        state = {
            "product": "BTC",
            "vol_phase": "filled_wait_close",
            "vol_direction": "long",
            "vol_entry_fill_ts": time.time() - 61.0,
            "vol_entry_fill_price": 100.0,
            "vol_entry_size": 1.0,
            "vol_close_digest": "d-lingering",
            "vol_close_posted_ts": 0.0,
        }
        client = _VolClient(
            mid=100.0,
            positions=[{"product_id": 2, "amount": 1.0, "side": "LONG"}],
        )
        with patch.object(volume_bot, "get_product_id", return_value=2), patch.object(
            volume_bot,
            "execute_limit_order",
            return_value={"success": True, "digest": "d-new-close", "price": 100.0},
        ):
            volume_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client)
        self.assertIn((2, "d-lingering"), client.cancelled)
        self.assertEqual(state.get("vol_close_digest"), "d-new-close")


class PerpRecycleEndToEndTests(unittest.TestCase):
    """The core complaint: after a close fills, state must reset so the next
    cycle can re-enter on the same margin."""

    def test_close_fill_resets_state_to_idle_and_clears_close_posted_ts(self):
        state = {
            "product": "BTC",
            "vol_phase": "pending_close_fill",
            "vol_direction": "long",
            "vol_entry_fill_price": 100.0,
            "vol_entry_size": 1.0,
            "vol_close_digest": "d-close",
            "vol_close_size": 1.0,
            "vol_close_posted_ts": time.time() - 30.0,
            "session_realized_pnl_usd": 0.0,
            "target_volume_usd": 10000.0,
        }
        client = _VolClient(mid=101.0, open_orders=[], positions=[])
        with patch.object(volume_bot, "get_product_id", return_value=2), patch.object(
            volume_bot,
            "query_order_by_digest",
            return_value={"fill_price": 101.0, "realized_pnl": 1.0, "fee": 0.01},
        ):
            result = volume_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client)
        self.assertTrue(result["success"])
        self.assertFalse(result.get("done"))
        self.assertEqual(state["vol_phase"], "idle")
        self.assertIsNone(state["vol_entry_digest"])
        self.assertIsNone(state["vol_close_digest"])
        # Critical for the recycle: the TTL clock must be cleared so the next
        # cycle starts from a clean slate rather than an expired timer.
        self.assertEqual(state["vol_close_posted_ts"], 0.0)


class SpotAliasTests(unittest.TestCase):
    """User-facing symbol aliasing for spot: 'BTC' must resolve to 'KBTC'
    without crashing when the Nado catalog only lists the K-prefixed name."""

    def test_btc_aliases_to_kbtc_when_allowed(self):
        state = {
            "product": "BTC",  # User typed BTC
            "vol_market": "spot",
            "vol_direction": "long",
        }
        client = _VolClient(mid=100_000.0)
        with patch.object(
            volume_bot, "list_volume_spot_product_names", return_value=["KBTC", "WETH", "USDC"],
        ), patch.object(volume_bot, "get_spot_product_id", return_value=42), patch.object(
            volume_bot, "get_spot_metadata", return_value={"symbol": "KBTC"}
        ), patch.object(
            volume_bot,
            "execute_spot_limit_order",
            return_value={"success": True, "digest": "s1", "price": 99900.0, "size": 0.001},
        ):
            result = volume_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client)
        self.assertTrue(result["success"])
        # The alias must be applied in state so future cycles use the resolved symbol.
        self.assertEqual(state["product"], "KBTC")

    def test_unknown_spot_symbol_returns_clear_error_not_crash(self):
        # Before the None-guard, `int(None)` would crash here.
        state = {"product": "DOGE", "vol_market": "spot", "vol_direction": "long"}
        client = _VolClient(mid=0.5)
        with patch.object(
            volume_bot, "list_volume_spot_product_names", return_value=["KBTC", "WETH", "USDC"],
        ):
            result = volume_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client)
        self.assertFalse(result["success"])
        self.assertIn("only", result["error"].lower())


class SpotBalanceRaceTests(unittest.TestCase):
    """If the balance endpoint hasn't indexed the fill yet, don't reset to idle
    and double-enter — wait the grace window first."""

    def test_spot_filled_wait_close_swallows_balance_race(self):
        now = time.time()
        state = {
            "product": "KBTC",
            "vol_market": "spot",
            "vol_phase": "filled_wait_close",
            "vol_direction": "long",
            # Close timer has expired so the flow is ready to post a close.
            "vol_entry_fill_ts": now - 61.0,
            "vol_entry_fill_price": 100000.0,
            "vol_entry_size": 0.001,
        }
        # Balance endpoint returns 0 (indexer lag). Grace window should kick in.
        client = _VolClient(mid=100000.0, spot_base_by_id={42: 0.0})
        with patch.object(
            volume_bot, "list_volume_spot_product_names", return_value=["KBTC"]
        ), patch.object(volume_bot, "get_spot_product_id", return_value=42), patch.object(
            volume_bot, "get_spot_metadata", return_value={"symbol": "KBTC"}
        ):
            # Temporarily override the grace window so this test isn't racey:
            # with default 15s and entry_ts = now - 61s we'd actually already be
            # out of grace. Bump the grace to cover the 61s.
            with patch.object(
                volume_bot, "SPOT_BALANCE_RACE_GRACE_SECONDS", 120.0
            ):
                result = volume_bot.run_cycle(telegram_id=1, network="mainnet", state=state, client=client)
        self.assertTrue(result["success"])
        self.assertEqual(result["action"], "waiting_balance_settle")
        # State MUST still be `filled_wait_close` — NOT reset to idle.
        self.assertEqual(state["vol_phase"], "filled_wait_close")


class AdaptiveCloseTtlTests(unittest.TestCase):
    def test_ttl_windows_widen_under_wide_spread_and_fast_move(self):
        state = {
            "adaptive_close_ttl": True,
            "vol_prev_mid": 95.0,
        }
        mp = {"bid": 99.0, "ask": 101.0, "mid": 100.0}
        repost_s, escalate_s = volume_bot._compute_close_ttl_windows(state, mp, 100.0)
        self.assertGreater(repost_s, volume_bot.CLOSE_REPOST_AFTER_SECONDS)
        self.assertGreater(escalate_s, volume_bot.CLOSE_ESCALATE_AFTER_SECONDS)


if __name__ == "__main__":
    unittest.main()
