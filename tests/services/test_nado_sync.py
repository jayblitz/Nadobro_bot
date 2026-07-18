import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.venue import nado_sync
from src.nadobro.utils.x18 import to_x18


class _Client:
    def __init__(self):
        self.summary_calls = 0
        self.include_isolated_flags = []

    async def calculate_account_summary(self, ts=None):
        self.summary_calls += 1
        return {
            "cross_positions": [
                {
                    "product_id": 1,
                    "symbol": "BTC",
                    "position_size": "1",
                    "notional_value": "100",
                    "est_pnl": "5",
                    "leverage": "2",
                    "initial_health": "10",
                    "maintenance_health": "8",
                }
            ],
            "isolated_positions": [],
        }

    def get_all_open_orders(self, refresh=True, *, include_isolated=True, strict=False):
        self.include_isolated_flags.append(include_isolated)
        return [{"product_id": 1, "product_name": "BTC", "digest": "0xabc", "amount": "1", "price": "100"}]

    async def get_trigger_orders(self, limit=200, strict=False):
        return [{"product_id": 1, "product_name": "BTC", "digest": "0xtrg", "amount": "1", "price": "110", "type": "trigger"}]

    async def get_matches(self, limit=200):
        return [{"submission_idx": "1", "base_filled": str(to_x18("1")), "quote_filled": str(to_x18("-100")), "fee": str(to_x18("1"))}]

    async def get_interest_and_funding_payments(self, limit=200):
        return [{"type": "funding", "product_id": 1, "timestamp": 1, "amount": str(to_x18("0.5"))}]

    def get_balance(self):
        # Spot balances feed the Total Balance line on the Overview deck.
        return {"exists": True, "balances": {0: "1500.00"}}


class NadoSyncTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        nado_sync.clear_cache()

    def tearDown(self):
        nado_sync.clear_cache()

    async def test_sync_user_populates_cache_and_coalesces_recent_reads(self):
        client = _Client()
        execute_calls = []

        with patch.object(nado_sync, "get_user", return_value=SimpleNamespace(network_mode=SimpleNamespace(value="testnet"))), patch.object(
            nado_sync, "get_user_nado_client", return_value=client
        ), patch.object(nado_sync, "execute", side_effect=lambda *a, **k: execute_calls.append(a)), patch.object(
            nado_sync, "query_one", return_value=None
        ):
            first = await nado_sync.sync_user(42, network="testnet", force=True)
            second = await nado_sync.sync_user(42, network="testnet", force=False)

        assert first["network"] == "testnet"
        assert first["positions"][0]["symbol"] == "BTC"
        assert len(first["open_orders"]) == 2
        assert first["open_orders"][1]["is_trigger"] is True
        assert first["stats"]["total_volume"] == 100
        assert first["equity"]["spot"] > 0  # Total Balance line gets spot from get_balance.
        assert second["positions"][0]["symbol"] == "BTC"
        assert client.summary_calls == 1
        assert execute_calls

    async def test_fill_nudge_subscribes_without_enabling_portfolio_ws(self):
        from src.nadobro.venue import nado_ws

        client = _Client()
        client.subaccount_hex = "0x" + "ab" * 32
        captured = []

        def _listener(_uid, _network):
            return None

        nado_ws._fill_listeners.clear()
        nado_ws.register_fill_listener(_listener)
        try:
            with patch.object(
                nado_sync, "get_user",
                return_value=SimpleNamespace(network_mode=SimpleNamespace(value="testnet")),
            ), patch.object(
                nado_sync, "get_user_nado_client", return_value=client,
            ), patch.object(
                nado_sync, "execute",
            ), patch.object(
                nado_sync, "query_one", return_value=None,
            ), patch.object(
                nado_sync, "portfolio_ws_enabled", return_value=False,
            ), patch.object(
                nado_sync, "fill_nudge_enabled", return_value=True,
            ), patch.object(
                nado_ws.portfolio_ws, "subscribe", side_effect=captured.append,
            ):
                await nado_sync.sync_user(42, network="testnet", force=True)
        finally:
            nado_ws._fill_listeners.clear()

        assert len(captured) == 1
        assert captured[0].user_id == 42
        assert captured[0].sync_portfolio is False

    async def test_sync_failure_marks_cached_snapshot_stale(self):
        cached = {"user_id": 42, "network": "testnet", "last_sync": datetime.now(timezone.utc), "positions": []}
        nado_sync.set_cached_snapshot(42, "testnet", cached)
        with patch.object(nado_sync, "get_user", return_value=SimpleNamespace(network_mode=SimpleNamespace(value="testnet"))), patch.object(
            nado_sync, "get_user_nado_client", side_effect=RuntimeError("boom")
        ), patch.object(nado_sync, "execute"):
            result = await nado_sync.sync_user(42, network="testnet", force=True)

        assert result["stale"] is True
        assert "boom" in result["error"]

    async def test_sync_summary_failure_does_not_write_destructive_snapshot(self):
        class FailingSummaryClient(_Client):
            async def calculate_account_summary(self, ts=None):
                raise RuntimeError("summary unavailable")

        cached = {"user_id": 42, "network": "testnet", "last_sync": datetime.now(timezone.utc), "positions": []}
        nado_sync.set_cached_snapshot(42, "testnet", cached)
        with patch.object(nado_sync, "get_user", return_value=SimpleNamespace(network_mode=SimpleNamespace(value="testnet"))), patch.object(
            nado_sync, "get_user_nado_client", return_value=FailingSummaryClient()
        ), patch.object(nado_sync, "_write_snapshot") as write_snapshot, patch.object(
            nado_sync, "_write_sync_log_error"
        ):
            result = await nado_sync.sync_user(42, network="testnet", force=True)

        assert result["stale"] is True
        assert "summary unavailable" in result["error"]
        write_snapshot.assert_not_called()

    async def test_circuit_open_returns_stale_for_refresh_without_db_writes(self):
        cached = {"user_id": 42, "network": "mainnet", "last_sync": datetime.now(timezone.utc), "positions": [{"symbol": "BTC"}]}
        nado_sync.set_cached_snapshot(42, "mainnet", cached)
        with patch.object(nado_sync, "get_user", return_value=SimpleNamespace(network_mode=SimpleNamespace(value="mainnet"))), patch.object(
            nado_sync, "_gateway_circuit_open", return_value=True
        ), patch.object(nado_sync, "get_user_nado_client") as client_spy, patch.object(nado_sync, "execute") as execute_spy:
            result = await nado_sync.sync_user(42, network="mainnet", reason="refresh", force=True)

        assert result["stale"] is True
        assert result["positions"] == [{"symbol": "BTC"}]
        client_spy.assert_not_called()
        execute_spy.assert_not_called()

    async def test_poll_sync_includes_isolated_orders_before_authoritative_sweep(self):
        client = _Client()
        stale_prior = {
            "user_id": 42,
            "network": "testnet",
            "monotonic_ts": 0,
            "positions": [{"symbol": "BTC", "isolated": False}],
            "open_orders": [{"digest": "0xold"}],
        }
        nado_sync.set_cached_snapshot(42, "testnet", stale_prior)

        with patch.object(nado_sync, "get_user", return_value=SimpleNamespace(network_mode=SimpleNamespace(value="testnet"))), patch.object(
            nado_sync, "get_user_nado_client", return_value=client
        ), patch.object(nado_sync, "execute"), patch.object(nado_sync, "query_one", return_value=None):
            await nado_sync.sync_user(42, network="testnet", reason="poll", max_age_ms=0)

        assert client.include_isolated_flags == [True]

    def test_write_matches_decodes_human_size_but_keeps_x18_fields(self):
        execute_calls = []
        with patch.object(nado_sync, "query_one", return_value=None), patch.object(
            nado_sync, "execute", side_effect=lambda *a, **k: execute_calls.append(a)
        ):
            inserted = nado_sync._write_matches(
                42,
                "testnet",
                [
                    {
                        "submission_idx": "2",
                        "product_id": 1,
                        "product_name": "BTC",
                        "base_filled": str(to_x18("1.25")),
                        "quote_filled": str(to_x18("-100")),
                        "fee": str(to_x18("0.5")),
                    }
                ],
            )

        assert inserted == 1
        params = execute_calls[0][1]
        # size + human columns (rollups read fill_size * fill_price, not x18):
        assert params[4] == "1.25"                  # size
        assert params[5] == "1.25"                  # fill_size (human)
        assert params[7] == "80"                    # fill_price = |quote|/|base| = 100/1.25
        assert params[8] == "0.5"                   # fill_fee (human)
        # x18 fields still persisted (authoritative venue values):
        assert params[12] == str(to_x18("0.5"))     # fee_x18
        assert params[13] == str(to_x18("1.25"))    # base_filled_x18

    def test_write_matches_enriches_manual_recorder_row_no_dupe(self):
        # A manual OPEN recorder row (which carries product_id) must be ENRICHED,
        # not duplicated as a product_id=0 'match' row.
        execute_calls = []

        def _q(sql, *params):
            # The enrich gate is now source-agnostic (matches any recorder row
            # for the digest); identify it by its NOT ILIKE '%close%' guard.
            if "NOT ILIKE" in sql:
                return {"id": 99}        # a recorder row exists for this digest
            return None                  # dedup / back-link / window: miss

        with patch.object(nado_sync, "query_one", side_effect=_q), patch.object(
            nado_sync, "execute", side_effect=lambda *a, **k: execute_calls.append(a)
        ):
            inserted = nado_sync._write_matches(42, "testnet", [{
                "submission_idx": "5", "digest": "0xabc",
                "base_filled": str(to_x18("1")), "quote_filled": str(to_x18("-100")),
                "fee": str(to_x18("0.1")),
            }])

        assert inserted == 1
        sql = execute_calls[0][0]
        assert "UPDATE" in sql and "INSERT INTO" not in sql  # enriched, not duplicated

    def test_write_matches_resolves_productid_from_open_orders_when_missing(self):
        # IndexerMatch has no product_id; for a fill with no recorder row (desk),
        # product_id + name are recovered from the live open_orders row.
        execute_calls = []

        def _q(sql, *params):
            if "FROM open_orders" in sql:
                return {"product_id": 4, "pair": "ETH-PERP"}
            return None                  # no recorder row, no dedup/intent hit

        with patch.object(nado_sync, "query_one", side_effect=_q), patch.object(
            nado_sync, "execute", side_effect=lambda *a, **k: execute_calls.append(a)
        ):
            inserted = nado_sync._write_matches(42, "testnet", [{
                "submission_idx": "7", "digest": "0xdesk",
                "base_filled": str(to_x18("2")), "quote_filled": str(to_x18("-200")),
                "fee": str(to_x18("0.2")),
            }])

        assert inserted == 1
        ins = next(c for c in execute_calls if "INSERT INTO" in c[0])
        params = ins[1]
        assert params[1] == 4            # product_id resolved from open_orders
        assert params[2] == "ETH-PERP"   # product_name from open_orders.pair

    def test_write_matches_recovers_productid_from_prior_trade_when_open_orders_gone(self):
        # A market/text-to-trade order fills instantly and leaves open_orders, so
        # its digest is gone by sync time. Recover product_id from a prior trades
        # row for the same digest — keeps text-to-trade fills OUT of the
        # product_id=0 bucket that History (get_paired_trades) excludes.
        execute_calls = []

        def _q(sql, *params):
            if "FROM open_orders" in sql:
                return None                                   # digest gone
            if "NOT ILIKE" in sql:
                return None                                   # no recorder row to enrich
            if "SELECT product_id, product_name, strategy_session_id FROM trades_" in sql:
                return {"product_id": 9, "product_name": "SOL-PERP"}  # prior OWN row
            return None                                       # dedup / back-link / window

        with patch.object(nado_sync, "query_one", side_effect=_q), patch.object(
            nado_sync, "execute", side_effect=lambda *a, **k: execute_calls.append(a)
        ):
            nado_sync._write_matches(42, "testnet", [{
                "submission_idx": "9", "digest": "0xmkt",
                "base_filled": str(to_x18("1")), "quote_filled": str(to_x18("-100")),
                "fee": str(to_x18("0.1")),
            }])

        ins = next(c for c in execute_calls if "INSERT INTO" in c[0])
        assert ins[1][1] == 9 and ins[1][2] == "SOL-PERP"

    def test_write_matches_inherits_productid_from_session_for_stop_close(self):
        # A stop-close fills after stopped_at with product_id=0, its digest gone
        # from open_orders and no prior recorder row — but it IS session-attributed
        # (the flatten was linked at placement). It must inherit the SESSION's
        # product_id so it counts toward the session's CLOSE volume (turnover =
        # opens + closes).
        def _q(sql, *params):
            if "FROM strategy_sessions" in sql:
                return {"product_id": 2, "product_name": "BTC"}
            return None  # open_orders / prior-trade / recorder-row / dedup all miss

        execute_calls = []
        with patch.object(nado_sync, "_back_link_intent", return_value=(90, "strategy", True, None, None)), \
             patch.object(nado_sync, "query_one", side_effect=_q), \
             patch.object(nado_sync, "execute", side_effect=lambda *a, **k: execute_calls.append(a)):
            nado_sync._write_matches(42, "mainnet", [{
                "submission_idx": "9", "digest": "0xclose",
                "base_filled": str(to_x18("0.0336")), "quote_filled": str(to_x18("-1997")),
                "fee": str(to_x18("0.3")),
            }])

        ins = next(c for c in execute_calls if "INSERT INTO" in c[0])
        params = ins[1]
        assert params[1] == 2 and params[2] == "BTC"   # product_id/name from session
        assert params[16] == 90                        # attributed to the session
        assert params[17] == "strategy"                # counts in the rollup

    def test_write_matches_window_fallback_skipped_for_tagged_manual_fill(self):
        # A fill whose digest is TAGGED manual (a bot manual trade, incl. closes)
        # must NOT be window-swallowed into a concurrent session on the same
        # product: that hid it from History (session NOT NULL) while the session
        # rollup still excluded it (source manual) — invisible everywhere.
        execute_calls = []
        window_calls = []

        with patch.object(nado_sync, "_back_link_intent", return_value=(None, "manual", True, None, None)), \
             patch.object(nado_sync, "_resolve_session_by_window",
                          side_effect=lambda *a, **k: window_calls.append(a) or 77), \
             patch.object(nado_sync, "query_one", return_value=None), \
             patch.object(nado_sync, "execute", side_effect=lambda *a, **k: execute_calls.append(a)):
            nado_sync._write_matches(42, "mainnet", [{
                "submission_idx": "11", "digest": "0xmanualclose", "product_id": 2,
                "base_filled": str(to_x18("0.5")), "quote_filled": str(to_x18("-50")),
                "fee": str(to_x18("0.05")),
            }])

        assert window_calls == []                      # fallback never ran
        ins = next(c for c in execute_calls if "INSERT INTO" in c[0])
        params = ins[1]
        assert params[16] is None                      # stays session-less
        assert params[17] == "manual"                  # stays in History's source set

    def test_write_matches_window_recovery_relabels_source_strategy(self):
        # An UNTAGGED fill recovered by the product+time window is an engine fill
        # whose placement link was lost. It must be labeled source='strategy' so
        # the session rollup (which excludes 'manual') actually counts it —
        # leaving it 'manual' made the recovery self-defeating.
        execute_calls = []

        with patch.object(nado_sync, "_back_link_intent", return_value=(None, "manual", False, None, None)), \
             patch.object(nado_sync, "_resolve_session_by_window", return_value=77), \
             patch.object(nado_sync, "query_one", return_value=None), \
             patch.object(nado_sync, "execute", side_effect=lambda *a, **k: execute_calls.append(a)):
            nado_sync._write_matches(42, "mainnet", [{
                "submission_idx": "12", "digest": "0xorphan", "product_id": 2,
                "base_filled": str(to_x18("0.5")), "quote_filled": str(to_x18("-50")),
                "fee": str(to_x18("0.05")),
            }])

        ins = next(c for c in execute_calls if "INSERT INTO" in c[0])
        params = ins[1]
        assert params[16] == 77                        # window-attributed session
        assert params[17] == "strategy"                # now counted by the rollup

    def test_write_open_orders_does_not_sweep_when_live_rows_have_no_digests(self):
        execute_calls = []
        with patch.object(nado_sync, "execute", side_effect=lambda *a, **k: execute_calls.append(a)):
            nado_sync._write_open_orders(42, "testnet", [{"product_id": 1, "product_name": "BTC"}])

        rendered_sql = "\n".join(call[0] for call in execute_calls)
        assert "cancelled_or_filled" not in rendered_sql

    def test_dedupe_orders_by_digest_keeps_first_occurrence(self):
        plain = [{"digest": "0xdup", "type": "LIMIT"}]
        trigger = [{"digest": "0xdup", "type": "TRIGGER", "order_type": "STOP"}]
        deduped = nado_sync._dedupe_orders_by_digest(plain + trigger)
        assert len(deduped) == 1
        assert deduped[0]["type"] == "LIMIT"

    def test_back_link_intent_rejects_cross_network_session(self):
        """A testnet fill must never inherit a mainnet session_id."""
        intent_row = {"value": {"strategy_session_id": 7, "source": "bro"}}
        session_row = {"network": "mainnet"}
        rows = [intent_row, session_row]

        with patch.object(nado_sync, "query_one", side_effect=lambda *a, **k: rows.pop(0) if rows else None):
            sid, src, found, _pid, _pname = nado_sync._back_link_intent("0xfeed", "testnet")

        assert sid is None
        assert src == "bro"
        assert found is True  # intent exists — window fallback must not run

    def test_back_link_intent_accepts_same_network_session(self):
        intent_row = {"value": {"strategy_session_id": 7, "source": "bro"}}
        session_row = {"network": "mainnet"}
        rows = [intent_row, session_row]

        with patch.object(nado_sync, "query_one", side_effect=lambda *a, **k: rows.pop(0) if rows else None):
            sid, src, found, _pid, _pname = nado_sync._back_link_intent("0xfeed", "mainnet")

        assert sid == 7
        assert src == "bro"
        assert found is True

    def test_window_session_resolver_matches_dn_spot_leg(self):
        ts = datetime(2026, 7, 5, tzinfo=timezone.utc)

        with patch.object(nado_sync, "query_one", return_value=None), \
             patch.object(nado_sync, "query_all", return_value=[{"id": 90, "product_name": "WGOOGLX"}]), \
             patch("src.nadobro.venue.product_catalog.get_dn_pair",
                   return_value={"perp_product_id": 117, "spot_product_id": 118}):
            sid = nado_sync._resolve_session_by_window(42, "mainnet", 118, ts)

        assert sid == 90

    def test_normalize_order_rows_treats_no_open_orders_message_as_empty(self):
        assert nado_sync._normalize_order_rows({"message": "No open orders"}) == []

    def test_resolve_leverage_returns_one_when_payload_missing_value(self):
        """Workflow plan §3: Nado summary frequently omits leverage; the
        legacy ``positions.leverage`` column is NOT NULL. ``_resolve_leverage``
        must always return a non-empty string so the insert never fails."""
        assert nado_sync._resolve_leverage({}) == "1"
        assert nado_sync._resolve_leverage({"leverage": None}) == "1"
        assert nado_sync._resolve_leverage({"leverage": ""}) == "1"

    def test_resolve_leverage_uses_payload_value_when_present(self):
        assert nado_sync._resolve_leverage({"leverage": "5"}) == "5"

    def test_resolve_leverage_derives_from_notional_and_margin_when_missing(self):
        """When Nado omits leverage but supplies notional + margin we can
        derive the implied account leverage (e.g. $1000 notional / $200
        margin -> 5x)."""
        out = nado_sync._resolve_leverage({"notional_value": "1000", "margin_used": "200"})
        assert out == "5"

    def test_back_link_intent_copies_session_and_source_from_intent(self):
        """Strategy fills arriving via venue sync don't carry the session
        tag at insert time; ``_back_link_intent`` recovers it via the
        order_digest -> order_intents lookup."""
        intent_row = {
            "value": {
                "strategy_session_id": 99,
                "source": "dgrid",
            }
        }
        rows = [intent_row, {"network": "testnet"}]
        with patch.object(nado_sync, "query_one", side_effect=lambda *a, **k: rows.pop(0) if rows else None):
            session_id, source, found, _pid, _pname = nado_sync._back_link_intent("0xdeadbeef", "testnet")
        assert session_id == 99
        assert source == "dgrid"
        assert found is True

    def test_back_link_intent_defaults_to_manual_when_lookup_empty(self):
        with patch.object(nado_sync, "query_one", return_value=None):
            session_id, source, found, _pid, _pname = nado_sync._back_link_intent("0xabc", "testnet")
        assert session_id is None
        assert source == "manual"
        assert found is False  # truly untagged — window fallback may recover it

    def test_user_has_isolated_artifacts_detects_isolated_position(self):
        """Poll fan-out must include isolated subaccounts whenever the prior
        snapshot shows any isolated artifact. Skipping it for users who hold
        no isolated positions is the optimization; keeping it for those who
        do is correctness."""
        assert nado_sync._user_has_isolated_artifacts(None) is False
        assert nado_sync._user_has_isolated_artifacts({}) is False
        cross_only = {
            "positions": [{"symbol": "BTC", "isolated": False}],
            "open_orders": [{"digest": "0x1"}],
        }
        assert nado_sync._user_has_isolated_artifacts(cross_only) is False
        with_isolated_position = {
            "positions": [{"symbol": "BTC", "isolated": True, "subaccount": "0xabc"}],
            "open_orders": [],
        }
        assert nado_sync._user_has_isolated_artifacts(with_isolated_position) is True
        with_isolated_order = {
            "positions": [],
            "open_orders": [{"digest": "0x2", "subaccount": "0xdef"}],
        }
        assert nado_sync._user_has_isolated_artifacts(with_isolated_order) is True

    def test_active_users_uses_cursor_pagination_not_limit_200(self):
        """SCALE: the previous active_users used ``LIMIT 200`` which silently
        dropped every other user once we crossed 200 simultaneously-active
        accounts. The new shape paginates by ``telegram_id > cursor`` with a
        page-sized limit so the scheduler walks the full set across ticks.
        """
        captured = {}

        def _capture(sql, params=None):
            captured["sql"] = sql
            captured["params"] = params
            return []

        with patch.object(nado_sync, "query_all", side_effect=_capture):
            nado_sync.active_users(limit=50, after_user_id=12345)

        assert "u.telegram_id > %s" in captured["sql"]
        assert "ORDER BY u.telegram_id ASC" in captured["sql"]
        assert "LIMIT %s" in captured["sql"]
        assert captured["params"] == (12345, 50)

    async def test_sync_active_users_advances_cursor_then_wraps(self):
        """Cursor must advance across ticks; reaching the end resets to 0 so
        new users (lower IDs added between ticks) eventually get picked up.
        """
        nado_sync._active_users_cursor = 0

        page_one = [
            {"telegram_id": 10, "network": "testnet"},
            {"telegram_id": 20, "network": "mainnet"},
        ]
        page_two = []  # end of set

        pages = iter([page_one, page_two])

        def _next_page(*args, **kwargs):
            return next(pages, [])

        async def _noop_sync(*args, **kwargs):
            return None

        with patch.object(nado_sync, "active_users", side_effect=_next_page), patch.object(
            nado_sync, "sync_user", new=_noop_sync,
        ):
            await nado_sync.sync_active_users(reason="test")
            assert nado_sync._active_users_cursor == 20  # advanced to last user id
            await nado_sync.sync_active_users(reason="test")
            assert nado_sync._active_users_cursor == 0  # wrapped when end reached

    async def test_per_user_timeout_capped_to_remaining_tick_budget(self):
        """A single wedged user must not run past the tick budget.

        Regression: ``_SYNC_USER_TIMEOUT_SECONDS`` (30s) was >= the sync
        interval (30s) and > the ~25.5s tick budget, so one wedged SDK call
        overran the whole interval and APScheduler skipped the next
        ``sync_active_users`` run. The per-user ``wait_for`` timeout is now
        bounded by the budget remaining in the tick.
        """
        nado_sync._active_users_cursor = 0
        captured_timeouts = []

        rows = [{"telegram_id": 10, "network": "mainnet"}]

        async def _fast_sync(*args, **kwargs):
            return None

        async def _capturing_wait_for(coro, timeout=None):
            captured_timeouts.append(timeout)
            return await coro  # consume the coroutine so it is awaited

        with patch.object(nado_sync, "active_users", return_value=rows), patch.object(
            nado_sync, "sync_user", new=_fast_sync
        ), patch.object(
            nado_sync, "portfolio_sync_interval_seconds", return_value=10
        ), patch.object(
            nado_sync.asyncio, "wait_for", new=_capturing_wait_for
        ):
            # tick_budget = max(5.0, 10 * 0.85) = 8.5s
            await nado_sync.sync_active_users(reason="test")

        assert captured_timeouts, "sync_user was never invoked"
        # Capped to the remaining budget (~8.5s), well below the 30s per-user
        # default — so the tick can never overrun its interval.
        assert captured_timeouts[0] <= 8.6
        assert captured_timeouts[0] < nado_sync._SYNC_USER_TIMEOUT_SECONDS

    async def test_budget_truncated_timeout_does_not_warn_wedged(self):
        """A timeout caused by the tick running out of budget (not a dead
        connection) must NOT log the scary "wedged SDK call" warning — that
        would re-create the log noise the per-user cap was meant to avoid. It
        is logged at debug as a retry-next-tick instead.
        """
        import asyncio as _asyncio

        nado_sync._active_users_cursor = 0
        rows = [{"telegram_id": 10, "network": "mainnet"}]

        async def _never_sync(*args, **kwargs):
            await _asyncio.sleep(100)

        async def _timeout_wait_for(coro, timeout=None):
            coro.close()  # avoid "coroutine was never awaited"
            raise _asyncio.TimeoutError

        with patch.object(nado_sync, "active_users", return_value=rows), patch.object(
            nado_sync, "sync_user", new=_never_sync
        ), patch.object(
            nado_sync, "portfolio_sync_interval_seconds", return_value=6
        ), patch.object(
            nado_sync.asyncio, "wait_for", new=_timeout_wait_for
        ):
            # tick_budget = max(5.0, 6*0.85) = 5.1s; user_timeout = ~5.1 < 30 =>
            # budget-truncated, not a wedge.
            with self.assertLogs("src.nadobro.venue.nado_sync", level="DEBUG") as cm:
                await nado_sync.sync_active_users(reason="test")

        joined = "\n".join(cm.output)
        assert "wedged" not in joined
        assert "truncated" in joined

    def test_write_matches_increments_session_win_count(self):
        execute_calls = []
        with patch.object(nado_sync, "query_one", return_value=None), patch.object(
            nado_sync, "execute", side_effect=lambda *a, **k: execute_calls.append(a)
        ), patch.object(
            nado_sync,
            "_back_link_intent",
            return_value=(7, "dgrid", True, None, None),
        ):
            nado_sync._write_matches(
                42,
                "testnet",
                [
                    {
                        "submission_idx": "3",
                        "product_id": 1,
                        "base_filled": "1000000000000000000",
                        "quote_filled": "-100000000000000000000",
                        "fee": "0",
                        "realized_pnl": "5000000000000000000",
                        "digest": "0xwin",
                    }
                ],
            )
        win_updates = [c for c in execute_calls if "win_count" in str(c[0])]
        assert win_updates


class AttributionRegression20260709Tests(unittest.TestCase):
    """Guardrails for the 2026-07-09 prod attribution audit.

    Root causes: (a) a session flatten whose order_intents write was lost
    orphaned as manual/unattributed (session #115 lost its close volume), and
    (b) bot close fills landed product_id=0 (invisible to History's round-trip
    pairing) because nothing carried the product for instantly-filled closes.
    """

    def _dispatch(self, handlers):
        def _q(sql, *params):
            for key, resp in handlers:
                if key in sql:
                    return resp
            return None
        return _q

    def test_write_matches_inherits_session_from_recorder_close_row_when_intent_missing(self):
        execute_calls = []
        handlers = [
            ("WHERE submission_idx = %s", None),                 # dedup: new fill
            ("FROM open_orders", None),                          # market close: gone
            ("COALESCE(product_id, 0) <> 0 ", {                  # recorder close row
                "product_id": 1, "product_name": "BTC",
                "strategy_session_id": 115,
            }),
            ("FROM order_intents", None),                        # intent write was lost
            ("NOT ILIKE", None),                                 # no enrichable row
        ]
        with patch.object(nado_sync, "query_one", side_effect=self._dispatch(handlers)), \
             patch.object(nado_sync, "execute", side_effect=lambda *a, **k: execute_calls.append(a)):
            inserted = nado_sync._write_matches(42, "mainnet", [{
                "submission_idx": "77", "digest": "0xflatten",
                "base_filled": str(to_x18("-0.00525")),
                "quote_filled": str(to_x18("329.80")),
                "fee": str(to_x18("0.14")),
            }])

        assert inserted == 1
        insert_calls = [c for c in execute_calls if "INSERT INTO" in c[0]]
        assert len(insert_calls) == 1
        params = insert_calls[0][1]
        assert params[1] == 1                 # product inherited, not ID:0
        assert params[2] == "BTC"
        assert params[16] == 115              # session inherited from recorder row
        assert params[17] == "strategy"       # counted by the session rollup

    def test_write_matches_resolves_product_from_intent_value(self):
        execute_calls = []
        handlers = [
            ("WHERE submission_idx = %s", None),
            ("FROM open_orders", None),
            ("COALESCE(product_id, 0) <> 0 ", None),             # no recorder row
            ("FROM order_intents", {"value": {
                "source": "manual", "product_id": 2, "product_name": "BTC-PERP",
            }}),
            ("NOT ILIKE", None),
        ]
        with patch.object(nado_sync, "query_one", side_effect=self._dispatch(handlers)), \
             patch.object(nado_sync, "execute", side_effect=lambda *a, **k: execute_calls.append(a)):
            inserted = nado_sync._write_matches(42, "mainnet", [{
                "submission_idx": "78", "digest": "0xmanualclose",
                "base_filled": str(to_x18("0.035")),
                "quote_filled": str(to_x18("-2200")),
                "fee": str(to_x18("0.72")),
            }])

        assert inserted == 1
        insert_calls = [c for c in execute_calls if "INSERT INTO" in c[0]]
        params = insert_calls[0][1]
        assert params[1] == 2                 # product recovered from the close tag
        assert params[2] == "BTC-PERP"
        assert params[16] is None             # manual close: no session
        assert params[17] == "manual"
