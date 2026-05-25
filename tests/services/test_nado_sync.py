import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.services import nado_sync
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
        assert params[4] == "1.25"
        assert params[8] == str(to_x18("0.5"))
        assert params[9] == str(to_x18("1.25"))

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
            sid, src = nado_sync._back_link_intent("0xfeed", "testnet")

        assert sid is None
        assert src == "bro"

    def test_back_link_intent_accepts_same_network_session(self):
        intent_row = {"value": {"strategy_session_id": 7, "source": "bro"}}
        session_row = {"network": "mainnet"}
        rows = [intent_row, session_row]

        with patch.object(nado_sync, "query_one", side_effect=lambda *a, **k: rows.pop(0) if rows else None):
            sid, src = nado_sync._back_link_intent("0xfeed", "mainnet")

        assert sid == 7
        assert src == "bro"

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
            session_id, source = nado_sync._back_link_intent("0xdeadbeef", "testnet")
        assert session_id == 99
        assert source == "dgrid"

    def test_back_link_intent_defaults_to_manual_when_lookup_empty(self):
        with patch.object(nado_sync, "query_one", return_value=None):
            session_id, source = nado_sync._back_link_intent("0xabc", "testnet")
        assert session_id is None
        assert source == "manual"

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

    def test_write_matches_increments_session_win_count(self):
        execute_calls = []
        with patch.object(nado_sync, "query_one", return_value=None), patch.object(
            nado_sync, "execute", side_effect=lambda *a, **k: execute_calls.append(a)
        ), patch.object(
            nado_sync,
            "_back_link_intent",
            return_value=(7, "dgrid"),
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
