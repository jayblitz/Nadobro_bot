"""Regressions for the 2026-07-18 copy-strategy audit fixes (F-1..F-6, F-10).

Each test pins a money-safety property the audit found missing, so a future
refactor that reintroduces the bug fails loudly.
"""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.nadobro.models import database as database_model
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


# ── F-1: a duplicate-suppressed result is never settled as a real close ──

def test_close_result_ok_rejects_a_duplicate_success():
    # This is the phantom-close / double-settle guard. A duplicate result
    # carries success=True but closed nothing (or carries another submit's
    # digest) — settling it corrupts PnL and hides live exposure.
    ok, err = copy_service._close_result_ok(
        {"success": True, "duplicate": True, "digest": None}
    )
    assert ok is False
    assert "duplicate" in err.lower()


def test_close_result_ok_accepts_a_real_fill_and_rejects_a_plain_failure():
    ok, _ = copy_service._close_result_ok({"success": True, "digest": "0xabc"})
    assert ok is True
    ok2, err2 = copy_service._close_result_ok({"success": False, "error": "boom"})
    assert ok2 is False and err2 == "boom"


def test_flatten_does_not_settle_a_duplicate_close():
    # A stop-flatten that gets a duplicate-suppressed close must leave the row
    # OPEN and report an error (so stop_requested keeps retrying) — never mark
    # it closed while the venue position is still live.
    cp = {"id": 1, "product_name": "BTC-PERP", "side": "long", "size": 1.0,
          "entry_price": 100.0, "leverage": 3.0}
    with patch.object(copy_service, "get_copy_mirror", return_value={"id": 5}), \
         patch.object(copy_service, "get_open_copy_positions", return_value=[cp]), \
         patch.object(copy_service, "execute_market_order",
                      return_value={"success": True, "duplicate": True, "digest": None}), \
         patch.object(copy_service, "_settle_copy_close") as settle, \
         patch.object(copy_service, "close_copy_position") as close_row:
        closed, _pnl, _vol, errors = copy_service._flatten_mirror_positions(5, 4242, "mainnet")

    assert closed == 0
    assert errors and "duplicate" in errors[0].lower()
    settle.assert_not_called()
    close_row.assert_not_called()


def test_market_order_releases_the_intent_on_definite_reject():
    # BEHAVIORAL: without the release the deterministic (nonce-free) copy close
    # intent stays 'recorded' for 120s and a legit retry gets a phantom
    # duplicate-success. Drive execute_market_order to the DEFINITE-reject
    # branch (submit ok, venue result success=False) and assert the intent is
    # released with status="failed".
    from types import SimpleNamespace
    from src.nadobro.trading import trade_service

    class _Client:
        def get_market_price(self, _pid):
            return {"mid": 100.0}

        def place_market_order(self, *a, **k):  # referenced by the (patched) submit
            raise AssertionError("submit is patched")

    intent_calls = []

    with patch.object(trade_service, "_builder_route_payload", return_value={}), \
         patch.object(trade_service, "validate_trade", return_value=(True, "")), \
         patch.object(trade_service, "get_user",
                      return_value=SimpleNamespace(network_mode=SimpleNamespace(value="mainnet"))), \
         patch.object(trade_service, "get_product_id", return_value=1), \
         patch.object(trade_service, "get_product_name", return_value="BTC-PERP"), \
         patch.object(trade_service, "get_user_nado_client", return_value=_Client()), \
         patch.object(trade_service, "get_trades_by_user", return_value=[]), \
         patch.object(trade_service, "is_product_isolated_only", return_value=False), \
         patch.object(trade_service, "insert_trade", return_value=99), \
         patch.object(trade_service, "update_trade"), \
         patch.object(trade_service, "update_trade_stats"), \
         patch.object(trade_service, "_submit_with_timeout",
                      return_value=(True, {"success": False, "error": "gateway throttle"})), \
         patch("src.nadobro.trading.order_intents.reserve_order_intent",
               return_value=(True, {"intent_id": "intent"})), \
         patch("src.nadobro.trading.order_intents.update_order_intent",
               side_effect=lambda intent_id, **kw: intent_calls.append((intent_id, kw))):
        result = trade_service.execute_market_order(
            telegram_id=42, product="BTC", size=1.0, is_long=True,
            source="copy", reduce_only=True,
        )

    assert result["success"] is False
    # The reject branch must release the intent to status="failed" (the
    # 'recorded' marker set pre-submit must not be the final state).
    failed = [c for c in intent_calls if c[1].get("status") == "failed"]
    assert failed, f"expected a status=failed intent release, got {intent_calls}"


# ── F-3: pending maker opens survive a restart ──

def test_pending_maker_opens_persist_and_restore():
    saved = {}
    with patch.object(copy_service, "set_bot_state", side_effect=lambda k, v: saved.update({k: v})):
        copy_service._register_pending_maker_open(5, 1, {"digest": "0xm", "user_id": 4242})
    assert copy_service._PENDING_STATE_KEY in saved
    assert "5:1" in saved[copy_service._PENDING_STATE_KEY]

    copy_service._PENDING_MAKER_OPENS.clear()
    with patch.object(copy_service, "get_bot_state",
                      return_value=saved[copy_service._PENDING_STATE_KEY]):
        copy_service._load_pending_maker_opens()
    assert (5, 1) in copy_service._PENDING_MAKER_OPENS
    assert copy_service._PENDING_MAKER_OPENS[(5, 1)]["digest"] == "0xm"


def test_pop_persists_the_shrunken_set():
    saved = {}
    with patch.object(copy_service, "set_bot_state", side_effect=lambda k, v: saved.update({k: v})):
        copy_service._register_pending_maker_open(5, 1, {"digest": "0xm"})
        copy_service._register_pending_maker_open(5, 2, {"digest": "0xn"})
        copy_service._pop_pending_maker_open(5, 1)
    assert set(saved[copy_service._PENDING_STATE_KEY].keys()) == {"5:2"}


# ── F-4: never open against an untrustworthy venue read ──

def test_open_skipped_when_follower_read_is_suspect():
    async def _case():
        mirror = {
            "id": 5, "user_id": 4242, "network": "mainnet", "active": True,
            "margin_per_trade": 100.0, "max_leverage": 3.0,
            "total_allocated_usd": 500.0,
        }

        async def _inline(fn, *args, **kwargs):
            return fn(*args, **kwargs)

        # follower client is None -> venue read unavailable -> suspect -> no open
        with patch.object(copy_service, "run_blocking", side_effect=_inline), \
             patch.object(copy_service, "get_open_copy_positions", return_value=[]), \
             patch.object(copy_service, "get_user_nado_client", return_value=None), \
             patch.object(copy_service, "set_mirror_unrealized"), \
             patch.object(copy_service, "get_product_name", return_value="BTC-PERP"), \
             patch.object(copy_service, "get_product_max_leverage", return_value=5.0), \
             patch.object(copy_service, "get_copy_mirror",
                          return_value=dict(mirror, stop_requested=False, paused=False)), \
             patch.object(copy_service, "_execute_maker_open") as maker, \
             patch.object(copy_service, "execute_market_order") as market:
            await copy_service._sync_mirror_positions(
                mirror, {1: {"side": "LONG", "entry_price": 100.0, "size": 1.0}},
            )

        maker.assert_not_called()
        market.assert_not_called()

    asyncio.run(_case())


def test_mass_vanish_extends_the_confirmation_window():
    # Every tracked position missing at once = read-outage signature. With the
    # normal (short) window already elapsed, an unproven close still must NOT
    # book under a mass-vanish; it needs the longer window.
    async def _case():
        mirror = {"id": 5, "user_id": 4242, "network": "mainnet"}
        rows = {
            1: {"id": 11, "product_id": 1, "product_name": "BTC-PERP", "side": "long",
                "entry_price": 100.0, "size": 1.0, "tp_order_digest": None, "sl_order_digest": None},
            2: {"id": 12, "product_id": 2, "product_name": "ETH-PERP", "side": "long",
                "entry_price": 50.0, "size": 2.0, "tp_order_digest": None, "sl_order_digest": None},
        }
        # Pre-seed suspects as first-seen 100s ago: long past the normal window,
        # short of the mass-vanish window.
        long_ago = time.time() - 100.0
        copy_service._EXTERNAL_CLOSE_SUSPECTS[(5, 1)] = long_ago
        copy_service._EXTERNAL_CLOSE_SUSPECTS[(5, 2)] = long_ago

        async def _inline(fn, *args, **kwargs):
            return fn(*args, **kwargs)

        client = MagicMock()
        client.get_market_price.return_value = {"mid": 90.0}
        with patch.object(copy_service, "run_blocking", side_effect=_inline), \
             patch.object(copy_service, "_archive_reads_unreliable", return_value=False), \
             patch.object(copy_service, "_resolve_bracket_fill", return_value=(0.0, 0.0, "external_close")), \
             patch.object(copy_service, "EXTERNAL_CLOSE_CONFIRM_SECONDS", 20.0), \
             patch.object(copy_service, "MASS_VANISH_CONFIRM_SECONDS", 200.0), \
             patch.object(copy_service, "update_mirror_accounting") as accounting, \
             patch.object(copy_service, "close_copy_position") as close_row, \
             patch.object(copy_service, "_notify_user", new_callable=AsyncMock):
            pnl, fees = await copy_service._sweep_external_closes(mirror, dict(rows), {}, client)

        assert (pnl, fees) == (0.0, 0.0)
        accounting.assert_not_called()
        close_row.assert_not_called()

    asyncio.run(_case())


def test_single_position_vanish_books_after_the_normal_window():
    # A lone position (no mass-vanish) uses the short window — proves the mass
    # guard doesn't over-block the ordinary case.
    async def _case():
        mirror = {"id": 5, "user_id": 4242, "network": "mainnet"}
        row = {"id": 11, "product_id": 1, "product_name": "BTC-PERP", "side": "long",
               "entry_price": 100.0, "size": 1.0, "tp_order_digest": None, "sl_order_digest": None}
        copy_service._EXTERNAL_CLOSE_SUSPECTS[(5, 1)] = time.time() - 100.0

        async def _inline(fn, *args, **kwargs):
            return fn(*args, **kwargs)

        client = MagicMock()
        client.get_market_price.return_value = {"mid": 105.0}
        with patch.object(copy_service, "run_blocking", side_effect=_inline), \
             patch.object(copy_service, "_archive_reads_unreliable", return_value=False), \
             patch.object(copy_service, "_resolve_bracket_fill", return_value=(0.0, 0.0, "external_close")), \
             patch.object(copy_service, "EXTERNAL_CLOSE_CONFIRM_SECONDS", 20.0), \
             patch.object(copy_service, "MASS_VANISH_CONFIRM_SECONDS", 200.0), \
             patch.object(copy_service, "update_mirror_accounting") as accounting, \
             patch.object(copy_service, "close_copy_position") as close_row, \
             patch.object(copy_service, "_notify_user", new_callable=AsyncMock):
            pnl, _fees = await copy_service._sweep_external_closes(mirror, {1: row}, {}, client)

        assert pnl == pytest.approx(5.0)
        accounting.assert_called_once()
        close_row.assert_called_once()

    asyncio.run(_case())


# ── F-4 clause 3 (TQ-1): silent-empty read while holding tracked positions ──

def test_open_skipped_when_read_empty_but_positions_are_tracked():
    async def _case():
        mirror = {
            "id": 5, "user_id": 4242, "network": "mainnet", "active": True,
            "margin_per_trade": 100.0, "max_leverage": 3.0, "total_allocated_usd": 500.0,
        }
        tracked = {"id": 9, "product_id": 2, "product_name": "ETH-PERP", "side": "long",
                   "entry_price": 50.0, "size": 1.0, "leverage": 3.0}

        async def _inline(fn, *args, **kwargs):
            return fn(*args, **kwargs)

        # We hold a tracked ETH row, but the follower read comes back EMPTY
        # (isolated discovery flaked) -> read is suspect -> the new BTC open
        # must be skipped even though venue_read_ok is True.
        follower = MagicMock()
        follower.get_all_positions.return_value = []
        follower.get_market_price.return_value = {"mid": 100.0}
        with patch.object(copy_service, "run_blocking", side_effect=_inline), \
             patch.object(copy_service, "get_open_copy_positions", return_value=[tracked]), \
             patch.object(copy_service, "get_user_nado_client", return_value=follower), \
             patch.object(copy_service, "_archive_reads_unreliable", return_value=False), \
             patch.object(copy_service, "_sweep_external_closes", new_callable=AsyncMock,
                          return_value=(0.0, 0.0)), \
             patch.object(copy_service, "set_mirror_unrealized"), \
             patch.object(copy_service, "get_product_name", return_value="BTC-PERP"), \
             patch.object(copy_service, "get_product_max_leverage", return_value=5.0), \
             patch.object(copy_service, "get_copy_mirror",
                          return_value=dict(mirror, stop_requested=False, paused=False)), \
             patch.object(copy_service, "_execute_maker_open") as maker:
            await copy_service._sync_mirror_positions(
                mirror, {1: {"side": "LONG", "entry_price": 100.0, "size": 1.0}},
            )

        maker.assert_not_called()

    asyncio.run(_case())


def test_open_skipped_when_archive_reads_unreliable():
    """F4-1: an archive rate-limit hides isolated positions, so opening blind
    could duplicate one — the read is suspect regardless of the client."""
    async def _case():
        mirror = {
            "id": 5, "user_id": 4242, "network": "mainnet", "active": True,
            "margin_per_trade": 100.0, "max_leverage": 3.0, "total_allocated_usd": 500.0,
        }

        async def _inline(fn, *args, **kwargs):
            return fn(*args, **kwargs)

        follower = MagicMock()
        follower.get_all_positions.return_value = []
        follower.get_market_price.return_value = {"mid": 100.0}
        with patch.object(copy_service, "run_blocking", side_effect=_inline), \
             patch.object(copy_service, "get_open_copy_positions", return_value=[]), \
             patch.object(copy_service, "get_user_nado_client", return_value=follower), \
             patch.object(copy_service, "_archive_reads_unreliable", return_value=True), \
             patch.object(copy_service, "_sweep_external_closes", new_callable=AsyncMock,
                          return_value=(0.0, 0.0)), \
             patch.object(copy_service, "set_mirror_unrealized"), \
             patch.object(copy_service, "get_product_name", return_value="BTC-PERP"), \
             patch.object(copy_service, "get_product_max_leverage", return_value=5.0), \
             patch.object(copy_service, "get_copy_mirror",
                          return_value=dict(mirror, stop_requested=False, paused=False)), \
             patch.object(copy_service, "_execute_maker_open") as maker:
            await copy_service._sync_mirror_positions(
                mirror, {1: {"side": "LONG", "entry_price": 100.0, "size": 1.0}},
            )

        maker.assert_not_called()

    asyncio.run(_case())


# ── F3-1 / F2-PAUSE-PENDING-ORPHAN (TQ-2): a PAUSED mirror's pending resolves ──

def test_poller_resolves_pending_for_a_paused_mirror_not_in_the_active_query():
    async def _case():
        # Active-mirror query returns NOTHING (mirror 5 is paused, excluded),
        # but a pending exists for it — the poller must still resolve it.
        copy_service._PENDING_MAKER_OPENS[(5, 1)] = {"digest": "0xm", "user_id": 4242}
        paused_mirror = {"id": 5, "user_id": 4242, "network": "mainnet",
                         "active": True, "paused": True, "stop_requested": False}

        async def _inline(fn, *args, **kwargs):
            return fn(*args, **kwargs)

        with patch.object(copy_service, "run_blocking", side_effect=_inline), \
             patch.object(copy_service, "get_all_active_mirrors_v2", return_value=[]), \
             patch.object(copy_service, "get_copy_mirror", return_value=paused_mirror), \
             patch.object(copy_service, "_resolve_pending_maker_opens_for_mirror",
                          new_callable=AsyncMock) as resolve:
            await copy_service._poll_all_mirrors()

        resolve.assert_awaited_once()
        assert resolve.await_args.args[0]["id"] == 5

    asyncio.run(_case())


def test_poller_drops_a_pending_with_no_mirror_row():
    async def _case():
        copy_service._PENDING_MAKER_OPENS[(99, 1)] = {"digest": "0xghost", "user_id": 4242}

        async def _inline(fn, *args, **kwargs):
            return fn(*args, **kwargs)

        with patch.object(copy_service, "run_blocking", side_effect=_inline), \
             patch.object(copy_service, "get_all_active_mirrors_v2", return_value=[]), \
             patch.object(copy_service, "get_copy_mirror", return_value=None), \
             patch.object(copy_service, "set_bot_state"), \
             patch.object(copy_service, "_resolve_pending_maker_opens_for_mirror",
                          new_callable=AsyncMock) as resolve:
            await copy_service._poll_all_mirrors()

        # Dangling pending (no mirror row) is cleared, not resolved.
        resolve.assert_not_awaited()
        assert (99, 1) not in copy_service._PENDING_MAKER_OPENS

    asyncio.run(_case())


# ── F-6: a stop_requested mirror stays pollable behind an inactive trader ──

def test_poller_query_keeps_stop_requested_mirrors_of_inactive_traders():
    with patch.object(database_model, "query_all", return_value=[]) as query_all:
        database_model.get_all_active_mirrors_v2()
    sql = query_all.call_args.args[0]
    assert "t.active = true OR m.stop_requested = true" in sql


# ── F-10: a never-acking cancel cannot deadlock the stop forever ──

def test_pending_hard_gives_up_when_cancel_never_acks():
    pending = {
        "digest": "0xmaker", "user_id": 4242,
        "created_ts": time.time() - (copy_service.MAKER_PENDING_GIVE_UP_SECONDS * 2) - 1,
        "cancel_confirmed": False,
    }
    client = MagicMock()
    client.cancel_order.return_value = {"success": False, "error": "venue busy"}
    with patch.object(copy_service, "query_orders_by_digests", return_value={}), \
         patch.object(copy_service, "get_user_nado_client", return_value=client):
        state, _ = copy_service._resolve_pending_maker_open("mainnet", 1, pending)

    assert state == "gone"  # unblocks the stop rather than looping forever
