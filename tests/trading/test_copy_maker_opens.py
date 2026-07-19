"""Maker-first copy opens: post-only at the touch, exact archive booking,
cancel-remainder, and the pending-order protocol for unknown fates."""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.nadobro.trading import copy_service


@pytest.fixture(autouse=True)
def _clean_runtime_state():
    copy_service._PENDING_MAKER_OPENS.clear()
    copy_service._EXTERNAL_CLOSE_SUSPECTS.clear()
    copy_service._TP_BREACH_FIRST_SEEN.clear()
    yield
    copy_service._PENDING_MAKER_OPENS.clear()
    copy_service._EXTERNAL_CLOSE_SUSPECTS.clear()
    copy_service._TP_BREACH_FIRST_SEEN.clear()


def _client(bid=100.0, ask=100.2, positions=None, cancel_success=True):
    client = MagicMock()
    client.get_market_price.return_value = {"bid": bid, "ask": ask, "mid": (bid + ask) / 2}
    client.get_all_positions.return_value = positions or []
    client.cancel_order.return_value = {"success": cancel_success} if cancel_success else {
        "success": False, "error": "venue busy"
    }
    return client


def _parsed(fill_size, original, price=100.0, fee=0.01):
    return {
        "fill_size": fill_size,
        "original_amount": original,
        "fill_price": price,
        "fee": fee,
        "builder_fee": 0.0,
        "is_filled": fill_size > 0,
    }


def test_maker_open_posts_at_touch_and_books_exact_archive_fill():
    client = _client(bid=100.0, ask=100.2)
    captured = {}

    def _capture_limit(**kwargs):
        captured.update(kwargs)
        return {"success": True, "digest": "0xmaker"}

    with patch.object(copy_service, "get_user_nado_client", return_value=client), \
         patch.object(copy_service, "execute_limit_order", side_effect=_capture_limit), \
         patch.object(copy_service, "query_orders_by_digests",
                      return_value={"0xmaker": _parsed(1.0, 1.0, price=100.0, fee=0.0)}):
        result = copy_service._execute_maker_open(
            user_id=4242, product_key="BTC", pid=1, size=1.0, is_long=True,
            leverage=3.0, network="mainnet", session_id=9,
        )

    assert captured["post_only"] is True
    assert captured["price"] == 100.0  # a long joins the BID, never crosses
    assert captured["source"] == "copy"
    assert result["success"] is True
    assert result["filled_size"] == 1.0
    assert result["price"] == 100.0
    assert result["fee"] == 0.0  # maker fee from the archive, not guessed
    client.cancel_order.assert_not_called()  # full fill needs no cancel


def test_maker_open_short_joins_the_ask():
    client = _client(bid=100.0, ask=100.2)
    captured = {}

    def _capture_limit(**kwargs):
        captured.update(kwargs)
        return {"success": True, "digest": "0xmaker"}

    with patch.object(copy_service, "get_user_nado_client", return_value=client), \
         patch.object(copy_service, "execute_limit_order", side_effect=_capture_limit), \
         patch.object(copy_service, "query_orders_by_digests",
                      return_value={"0xmaker": _parsed(1.0, -1.0)}):
        copy_service._execute_maker_open(
            user_id=4242, product_key="BTC", pid=1, size=1.0, is_long=False,
            leverage=3.0, network="mainnet", session_id=None,
        )

    assert captured["price"] == 100.2


def test_maker_open_partial_fill_cancels_remainder_and_keeps_the_filled_part():
    client = _client()
    with patch.object(copy_service, "get_user_nado_client", return_value=client), \
         patch.object(copy_service, "execute_limit_order",
                      return_value={"success": True, "digest": "0xmaker"}), \
         patch.object(copy_service, "MAKER_FILL_WAIT_SECONDS", 0.0), \
         patch.object(copy_service, "query_orders_by_digests",
                      return_value={"0xmaker": _parsed(0.4, 1.0, price=99.9, fee=0.004)}):
        result = copy_service._execute_maker_open(
            user_id=4242, product_key="BTC", pid=1, size=1.0, is_long=True,
            leverage=3.0, network="mainnet", session_id=None,
        )

    client.cancel_order.assert_called_once_with(1, "0xmaker")
    assert result["success"] is True
    assert result["filled_size"] == 0.4
    assert result["price"] == 99.9


def test_maker_open_zero_fill_confirms_flat_on_venue_then_reports_unfilled():
    # The archive does NOT index zero-fill orders (verified live), so the
    # unfilled verdict must come from engine state: cancel acked + follower
    # still flat on the product.
    client = _client(positions=[])
    with patch.object(copy_service, "get_user_nado_client", return_value=client), \
         patch.object(copy_service, "execute_limit_order",
                      return_value={"success": True, "digest": "0xmaker"}), \
         patch.object(copy_service, "MAKER_FILL_WAIT_SECONDS", 0.0), \
         patch.object(copy_service, "_archive_reads_unreliable", return_value=False), \
         patch.object(copy_service, "query_orders_by_digests", return_value={}):
        result = copy_service._execute_maker_open(
            user_id=4242, product_key="BTC", pid=1, size=1.0, is_long=True,
            leverage=3.0, network="mainnet", session_id=None,
        )

    assert result == {"success": False, "error": "maker_unfilled"}


def test_maker_open_unknown_fate_goes_pending_never_silent():
    # Cancel refused AND archive silent: the order may be live. The caller
    # must get pending=True so it locks the product against a second order.
    client = _client(cancel_success=False)
    with patch.object(copy_service, "get_user_nado_client", return_value=client), \
         patch.object(copy_service, "execute_limit_order",
                      return_value={"success": True, "digest": "0xmaker"}), \
         patch.object(copy_service, "MAKER_FILL_WAIT_SECONDS", 0.0), \
         patch.object(copy_service, "_archive_reads_unreliable", return_value=False), \
         patch.object(copy_service, "query_orders_by_digests", return_value={}):
        result = copy_service._execute_maker_open(
            user_id=4242, product_key="BTC", pid=1, size=1.0, is_long=True,
            leverage=3.0, network="mainnet", session_id=None,
        )

    assert result["pending"] is True
    assert result["digest"] == "0xmaker"
    assert result["cancel_confirmed"] is False


def test_still_flat_check_is_skipped_while_archive_reads_are_unreliable():
    # Under archive rate-limit the positions read may be missing isolated
    # positions — "flat" is not trustworthy, so the order must go pending.
    client = _client(positions=[])
    with patch.object(copy_service, "get_user_nado_client", return_value=client), \
         patch.object(copy_service, "execute_limit_order",
                      return_value={"success": True, "digest": "0xmaker"}), \
         patch.object(copy_service, "MAKER_FILL_WAIT_SECONDS", 0.0), \
         patch.object(copy_service, "_archive_reads_unreliable", return_value=True), \
         patch.object(copy_service, "query_orders_by_digests", return_value={}):
        result = copy_service._execute_maker_open(
            user_id=4242, product_key="BTC", pid=1, size=1.0, is_long=True,
            leverage=3.0, network="mainnet", session_id=None,
        )

    assert result["pending"] is True


def test_pending_resolution_books_a_late_fill_exactly():
    async def _case():
        mirror = {"id": 5, "user_id": 4242, "network": "mainnet", "strategy_session_id": 9}
        copy_service._PENDING_MAKER_OPENS[(5, 1)] = {
            "digest": "0xmaker", "user_id": 4242, "product_key": "BTC",
            "product_name": "BTC-PERP", "is_long": True, "leverage": 3.0,
            "leader_entry": 100.0, "leader_size": 2.0,
            "created_ts": time.time(), "cancel_confirmed": True,
        }

        async def _inline(fn, *args, **kwargs):
            return fn(*args, **kwargs)

        inserted = {}

        def _capture_insert(data):
            inserted.update(data)
            return 321

        with patch.object(copy_service, "run_blocking", side_effect=_inline), \
             patch.object(copy_service, "query_orders_by_digests",
                          return_value={"0xmaker": _parsed(1.0, 1.0, price=99.5, fee=0.002)}), \
             patch.object(copy_service, "update_mirror_accounting") as accounting, \
             patch.object(copy_service, "insert_copy_position", side_effect=_capture_insert), \
             patch.object(copy_service, "get_copy_mirror",
                          return_value={"id": 5, "active": True, "stop_requested": False}), \
             patch.object(copy_service, "request_copy_mirror_stop") as rearm, \
             patch.object(copy_service, "_notify_user", new_callable=AsyncMock):
            all_resolved = await copy_service._resolve_pending_maker_opens_for_mirror(mirror)

        assert all_resolved is True
        assert (5, 1) not in copy_service._PENDING_MAKER_OPENS
        assert inserted["entry_price"] == 99.5
        assert inserted["size"] == 1.0
        assert inserted["side"] == "long"
        accounting.assert_called_once_with(5, fees_delta=0.002, volume_delta=99.5 * 1.0)
        # Mirror still active -> no stop re-arm on the happy path.
        rearm.assert_not_called()

    asyncio.run(_case())


def test_late_fill_under_a_stopped_mirror_rearms_the_stop():
    """COPY-STOP-RACE (F-2, second window): a pending that resolves to a fill
    while the mirror was stopped mid-resolution must re-arm the stop so the
    freshly-booked row gets flattened, not orphaned."""
    async def _case():
        mirror = {"id": 5, "user_id": 4242, "network": "mainnet", "strategy_session_id": 9}
        copy_service._PENDING_MAKER_OPENS[(5, 1)] = {
            "digest": "0xmaker", "user_id": 4242, "product_key": "BTC",
            "product_name": "BTC-PERP", "is_long": True, "leverage": 3.0,
            "leader_entry": 100.0, "leader_size": 2.0,
            "created_ts": time.time(), "cancel_confirmed": True,
        }

        async def _inline(fn, *args, **kwargs):
            return fn(*args, **kwargs)

        with patch.object(copy_service, "run_blocking", side_effect=_inline), \
             patch.object(copy_service, "query_orders_by_digests",
                          return_value={"0xmaker": _parsed(1.0, 1.0, price=99.5, fee=0.002)}), \
             patch.object(copy_service, "update_mirror_accounting"), \
             patch.object(copy_service, "insert_copy_position", return_value=321), \
             patch.object(copy_service, "get_copy_mirror",
                          return_value={"id": 5, "active": False, "stop_requested": False}), \
             patch.object(copy_service, "request_copy_mirror_stop") as rearm, \
             patch.object(copy_service, "_notify_user", new_callable=AsyncMock):
            await copy_service._resolve_pending_maker_opens_for_mirror(mirror)

        rearm.assert_called_once_with(5)

    asyncio.run(_case())


def test_pending_resolution_gone_when_cancel_acked_and_flat():
    pending = {
        "digest": "0xmaker", "user_id": 4242, "created_ts": time.time(),
        "cancel_confirmed": True,
    }
    client = _client(positions=[])
    with patch.object(copy_service, "query_orders_by_digests", return_value={}), \
         patch.object(copy_service, "_archive_reads_unreliable", return_value=False), \
         patch.object(copy_service, "get_user_nado_client", return_value=client):
        state, result = copy_service._resolve_pending_maker_open("mainnet", 1, pending)

    assert state == "gone"
    assert result is None


def test_pending_resolution_stays_unknown_while_nothing_is_confirmable():
    pending = {
        "digest": "0xmaker", "user_id": 4242, "created_ts": time.time(),
        "cancel_confirmed": True,
    }
    with patch.object(copy_service, "query_orders_by_digests", return_value={}), \
         patch.object(copy_service, "_archive_reads_unreliable", return_value=True):
        state, result = copy_service._resolve_pending_maker_open("mainnet", 1, pending)

    assert state == "unknown"
    assert result is None


def test_pending_resolution_gives_up_loudly_after_the_deadline():
    pending = {
        "digest": "0xmaker", "user_id": 4242,
        "created_ts": time.time() - copy_service.MAKER_PENDING_GIVE_UP_SECONDS - 1,
        "cancel_confirmed": True,
    }
    with patch.object(copy_service, "query_orders_by_digests", return_value={}), \
         patch.object(copy_service, "_archive_reads_unreliable", return_value=True):
        state, _ = copy_service._resolve_pending_maker_open("mainnet", 1, pending)

    assert state == "gone"


def test_flatten_refuses_a_clean_result_while_a_maker_open_is_pending():
    copy_service._PENDING_MAKER_OPENS[(5, 1)] = {"digest": "0xmaker", "user_id": 4242}
    with patch.object(copy_service, "get_copy_mirror", return_value={"id": 5}), \
         patch.object(copy_service, "get_open_copy_positions", return_value=[]):
        closed, _pnl, _vol, errors = copy_service._flatten_mirror_positions(5, 4242, "mainnet")

    assert closed == 0
    assert errors and "unresolved" in errors[0]
