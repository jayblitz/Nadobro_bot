"""Copy-stop regressions: no hidden exposure after a failed close."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

from src.nadobro.models import database as database_model
from src.nadobro.trading import copy_service


def test_user_stop_keeps_failed_flatten_monitored_for_retry():
    mirror = {"id": 7, "user_id": 4242, "active": True, "network": "mainnet"}
    with patch.object(copy_service, "get_copy_mirror", return_value=mirror), \
         patch.object(copy_service, "_flatten_mirror_positions", return_value=(0, 0.0, 0.0, ["BTC: venue down"])), \
         patch.object(copy_service, "request_copy_mirror_stop") as request_stop, \
         patch.object(copy_service, "stop_copy_mirror") as stop, \
         patch.object(copy_service, "_finalize_mirror_session") as finalize:
        ok, message = copy_service.stop_copy(4242, 7)

    assert not ok
    assert "remains monitored" in message
    request_stop.assert_called_once_with(7)
    stop.assert_not_called()
    finalize.assert_not_called()


def test_stop_all_reports_failure_when_every_copy_close_is_pending():
    mirrors = [
        {"id": 7, "user_id": 4242, "active": True, "network": "mainnet"},
        {"id": 8, "user_id": 4242, "active": True, "network": "testnet"},
    ]
    with patch.object(copy_service, "get_user_active_mirrors_v2", return_value=mirrors), \
         patch.object(
             copy_service,
             "_flatten_mirror_positions",
             side_effect=[
                 (0, 0.0, 0.0, ["BTC: venue down"]),
                 (0, 0.0, 0.0, ["ETH: venue down"]),
             ],
         ), \
         patch.object(copy_service, "request_copy_mirror_stop") as request_stop, \
         patch.object(copy_service, "stop_copy_mirror") as stop, \
         patch.object(copy_service, "_finalize_mirror_session") as finalize:
        ok, message = copy_service.stop_all_copies(4242)

    assert not ok
    assert "Stopped 0 copy mirror(s)" in message
    assert "2 mirror(s) remain monitored" in message
    assert request_stop.call_count == 2
    request_stop.assert_any_call(7)
    request_stop.assert_any_call(8)
    stop.assert_not_called()
    finalize.assert_not_called()


def test_stop_all_reports_failure_for_mixed_stopped_and_pending_copies():
    mirrors = [
        {"id": 7, "user_id": 4242, "active": True, "network": "mainnet"},
        {"id": 8, "user_id": 4242, "active": True, "network": "testnet"},
    ]
    with patch.object(copy_service, "get_user_active_mirrors_v2", return_value=mirrors), \
         patch.object(
             copy_service,
             "_flatten_mirror_positions",
             side_effect=[
                 (1, 4.0, 100.0, []),
                 (0, 0.0, 0.0, ["ETH: venue down"]),
             ],
         ), \
         patch.object(copy_service, "request_copy_mirror_stop") as request_stop, \
         patch.object(copy_service, "stop_copy_mirror") as stop, \
         patch.object(copy_service, "_finalize_mirror_session") as finalize:
        ok, message = copy_service.stop_all_copies(4242)

    assert not ok
    assert "Stopped 1 copy mirror(s)" in message
    assert "Flattened 1 copied position(s)" in message
    assert "1 mirror(s) remain monitored" in message
    stop.assert_called_once_with(7)
    finalize.assert_called_once_with(mirrors[0], "user_stop_all")
    request_stop.assert_called_once_with(8)


def test_stop_request_unpauses_a_mirror_so_the_retry_poller_can_see_it():
    with patch.object(database_model, "execute") as execute:
        database_model.request_copy_mirror_stop(7)

    sql, params = execute.call_args.args
    assert "active = true" in sql
    assert "stop_requested = true" in sql
    assert "paused = false" in sql
    assert params == (7,)


def test_pending_stop_cannot_be_paused_or_resumed_by_a_stale_callback():
    mirror = {
        "id": 7,
        "user_id": 4242,
        "active": True,
        "paused": False,
        "stop_requested": True,
    }
    with patch.object(copy_service, "get_copy_mirror", return_value=mirror), \
         patch.object(copy_service, "pause_copy_mirror") as pause, \
         patch.object(copy_service, "resume_copy_mirror") as resume:
        paused, pause_message = copy_service.pause_copy(4242, 7)
        resumed, resume_message = copy_service.resume_copy(4242, 7)

    assert not paused
    assert not resumed
    assert "stopping" in pause_message
    assert "stopping" in resume_message
    pause.assert_not_called()
    resume.assert_not_called()


def test_database_pause_and_poller_queries_keep_stop_retries_visible():
    with patch.object(database_model, "execute") as execute, \
         patch.object(database_model, "query_all", return_value=[]) as query_all:
        database_model.pause_copy_mirror(7)
        database_model.get_all_active_mirrors_v2()

    assert "AND NOT stop_requested" in execute.call_args.args[0]
    assert "NOT m.paused OR m.stop_requested = true" in query_all.call_args.args[0]


def test_pending_stop_cannot_be_restarted_over_open_copy_exposure():
    user = type("User", (), {"linked_signer_address": "0xsigner"})()
    trader = {"id": 9, "wallet_address": "0xleader", "label": "leader", "active": True, "owner_user_id": 4242}
    pending = {"id": 7, "trader_id": 9, "stop_requested": True, "active": True}
    with patch.object(copy_service, "get_user", return_value=user), \
         patch.object(copy_service, "get_copy_trader", return_value=trader), \
         patch.object(copy_service, "get_user_active_mirrors_v2", return_value=[pending]), \
         patch.object(copy_service, "create_copy_mirror_v2") as create:
        ok, message = copy_service.start_copy(4242, 9, margin_per_trade=100.0)

    assert not ok
    assert "still closing" in message
    create.assert_not_called()


def test_atomic_mirror_create_refuses_pending_stop_after_locking_trader():
    class Cursor:
        def __init__(self):
            self.calls = []
            self._rows = iter(({"id": 9}, {"id": 7}))

        def execute(self, sql, params=None):
            self.calls.append((sql, params))

        def fetchone(self):
            return next(self._rows)

    cursor = Cursor()
    with patch.object(database_model, "run_transaction", side_effect=lambda work: work(cursor)):
        mirror_id = database_model.create_copy_mirror_v2(
            user_id=4242,
            trader_id=9,
            network="mainnet",
            margin_per_trade=100.0,
        )

    assert mirror_id is None
    assert "FOR UPDATE" in cursor.calls[0][0]
    assert "stop_requested = true" in cursor.calls[1][0]
    assert "copy_positions" in cursor.calls[1][0]
    assert "stop_requested = true" in cursor.calls[2][0]
    assert not any("INSERT INTO copy_mirrors" in sql for sql, _ in cursor.calls)


def test_remove_refuses_to_hide_a_trader_when_a_close_fails():
    trader = {"id": 9, "wallet_address": "0xabc", "label": "leader", "owner_user_id": None}
    mirror = {"id": 7, "user_id": 4242, "network": "mainnet", "active": True}
    with patch.object(copy_service, "get_copy_trader", return_value=trader), \
         patch.object(copy_service, "get_mirrors_for_trader", return_value=[mirror]), \
         patch.object(copy_service, "_flatten_mirror_positions", return_value=(0, 0.0, 0.0, ["BTC: venue down"])), \
         patch.object(copy_service, "request_copy_mirror_stop") as request_stop, \
         patch.object(copy_service, "stop_copy_mirror") as stop, \
         patch.object(copy_service, "_finalize_mirror_session") as finalize, \
         patch.object(copy_service, "deactivate_copy_trader") as deactivate:
        ok, message = copy_service.remove_trader(9, requester_user_id=1, is_admin=True)

    assert not ok
    assert "remains active" in message
    request_stop.assert_called_once_with(7)
    stop.assert_not_called()
    finalize.assert_not_called()
    deactivate.assert_not_called()


def test_stop_retry_does_not_depend_on_a_leader_portfolio_read():
    async def _case():
        mirror = {
            "id": 7,
            "user_id": 4242,
            "trader_id": 9,
            "network": "mainnet",
            "wallet_address": "0xleader",
            "stop_requested": True,
        }

        async def _inline(fn, *args, **kwargs):
            if fn.__name__ == "get_all_active_mirrors_v2":
                return [mirror]
            if fn.__name__ == "_load_leader_position_map":
                raise AssertionError("stop retry must not read the leader portfolio")
            raise AssertionError(f"Unexpected blocking function: {fn.__name__}")

        with patch.object(copy_service, "run_blocking", side_effect=_inline), \
             patch.object(copy_service, "_sync_mirror_positions", new_callable=AsyncMock) as sync:
            await copy_service._poll_all_mirrors()

        sync.assert_awaited_once_with(mirror, {})

    asyncio.run(_case())


def test_stop_requested_mirror_retries_flatten_without_mirroring_new_opens():
    async def _case():
        mirror = {
            "id": 7,
            "user_id": 4242,
            "network": "testnet",
            "active": True,
            "stop_requested": True,
        }

        async def _inline(fn, *args, **kwargs):
            return fn(*args, **kwargs)

        with patch.object(copy_service, "run_blocking", side_effect=_inline), \
             patch.object(copy_service, "_flatten_mirror_positions", return_value=(1, 4.0, 100.0, [])), \
             patch.object(copy_service, "stop_copy_mirror") as stop, \
             patch.object(copy_service, "_finalize_mirror_session") as finalize, \
             patch.object(copy_service, "_notify_user", new_callable=AsyncMock) as notify:
            await copy_service._sync_mirror_positions(mirror, {1: {"side": "LONG", "size": 1.0}})

        stop.assert_called_once_with(7)
        finalize.assert_called_once_with(mirror, "user_stop")
        notify.assert_awaited_once()

    asyncio.run(_case())


def test_network_switch_does_not_stop_a_network_scoped_mirror():
    async def _case():
        mirror = {
            "id": 7,
            "user_id": 4242,
            "network": "testnet",
            "active": True,
            "margin_per_trade": 100.0,
            "max_leverage": 3.0,
            "total_allocated_usd": 500.0,
        }

        async def _inline(fn, *args, **kwargs):
            return fn(*args, **kwargs)

        with patch.object(copy_service, "run_blocking", side_effect=_inline), \
             patch.object(copy_service, "get_open_copy_positions", return_value=[]), \
             patch.object(copy_service, "get_user_nado_client", return_value=None) as client, \
             patch.object(copy_service, "set_mirror_unrealized"), \
             patch.object(copy_service, "auto_stop_mirror") as auto_stop, \
             patch.object(copy_service, "_flatten_mirror_positions") as flatten:
            await copy_service._sync_mirror_positions(mirror, {})

        client.assert_called_once_with(4242, network="testnet")
        auto_stop.assert_not_called()
        flatten.assert_not_called()

    asyncio.run(_case())
