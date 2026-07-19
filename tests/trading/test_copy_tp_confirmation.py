"""COPY-TP-CONFIRM: the profit-side rail needs a conservative numerator and a
second confirming poll; the SL rail keeps firing immediately (untouched)."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
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


def _mirror(**overrides):
    base = {
        "id": 5,
        "user_id": 4242,
        "network": "mainnet",
        "margin_per_trade": 100.0,
        "max_leverage": 3.0,
        "cumulative_stop_loss_pct": 0.0,
        "cumulative_take_profit_pct": 50.0,
        "total_allocated_usd": 500.0,
        "cumulative_pnl": 0.0,
        "cumulative_fees_usd": 0.0,
    }
    base.update(overrides)
    return base


async def _inline(fn, *args, **kwargs):
    return fn(*args, **kwargs)


def _run_sync(mirror, leader_map=None, client=None):
    async def _case():
        get_client = (
            patch.object(copy_service, "get_user_nado_client", return_value=client)
            if client is not None
            else patch.object(copy_service, "get_user_nado_client", side_effect=RuntimeError("no client"))
        )
        with patch.object(copy_service, "run_blocking", side_effect=_inline), \
             patch.object(copy_service, "get_open_copy_positions",
                          return_value=mirror.pop("_open_rows", [])), \
             get_client, \
             patch.object(copy_service, "set_mirror_unrealized") as set_upnl, \
             patch.object(copy_service, "_flatten_mirror_positions",
                          return_value=(1, 0.0, 0.0, [])) as flatten, \
             patch.object(copy_service, "auto_stop_mirror") as auto_stop, \
             patch.object(copy_service, "_finalize_mirror_session"), \
             patch.object(copy_service, "_notify_user", new_callable=AsyncMock):
            await copy_service._sync_mirror_positions(dict(mirror), leader_map or {})
        return flatten, auto_stop, set_upnl

    return asyncio.run(_case())


def test_tp_needs_a_second_confirming_poll():
    mirror = _mirror(cumulative_pnl=300.0)  # +60% of $500, target 50%
    with patch.object(copy_service, "TP_CONFIRM_GAP_SECONDS", 0.0):
        flatten1, auto_stop1, _ = _run_sync(dict(mirror))
        assert flatten1.call_count == 0, "first breached poll must only arm the marker"
        assert auto_stop1.call_count == 0
        assert 5 in copy_service._TP_BREACH_FIRST_SEEN

        flatten2, auto_stop2, _ = _run_sync(dict(mirror))
        assert flatten2.call_count == 1, "second breached poll fires the TP"
        auto_stop2.assert_called_once()
        assert "profit hit 60.0%" in auto_stop2.call_args.args[1]


def test_sl_still_fires_on_the_first_poll():
    # GUARDRAIL per product decision: SL behavior is untouched — immediate,
    # primary estimator. A confirmation delay on a stop-loss costs money.
    mirror = _mirror(cumulative_pnl=-300.0, cumulative_stop_loss_pct=50.0,
                     cumulative_take_profit_pct=0.0)
    flatten, auto_stop, _ = _run_sync(mirror)
    assert flatten.call_count == 1
    auto_stop.assert_called_once()
    assert "loss hit 60.0%" in auto_stop.call_args.args[1]


def test_tp_judges_the_conservative_upnl_not_the_venue_spike():
    # Venue uPnL claims +$500 (100% of budget); the mid-derived pairing says
    # +$10. TP must not fire off the unreproducible venue number — while the
    # dashboard/SL basis (primary) still records the venue estimate.
    open_row = {
        "id": 77, "product_id": 1, "product_name": "BTC-PERP", "side": "long",
        "entry_price": 100.0, "size": 1.0, "leverage": 3.0,
    }
    client = MagicMock()
    client.get_all_positions.return_value = [
        {"product_id": 1, "amount": 1.0, "unrealized_pnl": 500.0}
    ]
    client.get_market_price.return_value = {"bid": 109.9, "ask": 110.1, "mid": 110.0}

    mirror = _mirror()
    mirror["_open_rows"] = [open_row]
    with patch.object(copy_service, "TP_CONFIRM_GAP_SECONDS", 0.0), \
         patch.object(copy_service, "_archive_reads_unreliable", return_value=False):
        flatten1, auto_stop1, set_upnl1 = _run_sync(dict(mirror), client=client)
        mirror2 = _mirror()
        mirror2["_open_rows"] = [dict(open_row)]
        flatten2, auto_stop2, _ = _run_sync(mirror2, client=client)

    assert flatten1.call_count == 0 and flatten2.call_count == 0
    assert auto_stop1.call_count == 0 and auto_stop2.call_count == 0
    # Primary estimator (venue-scaled) still feeds the dashboard snapshot.
    set_upnl1.assert_called_once_with(5, 500.0)
    # Conservative numerator ($10 = mid pairing) never breached, so no marker.
    assert 5 not in copy_service._TP_BREACH_FIRST_SEEN


def test_tp_marker_disarms_when_the_breach_dips_away():
    assert copy_service._tp_breach_confirmed(5, True, now=1000.0) is False   # arm
    assert copy_service._tp_breach_confirmed(5, False, now=1030.0) is False  # dip -> disarm
    with patch.object(copy_service, "TP_CONFIRM_GAP_SECONDS", 0.0):
        assert copy_service._tp_breach_confirmed(5, True, now=1060.0) is False  # re-arm only
        assert copy_service._tp_breach_confirmed(5, True, now=1090.0) is True


def test_tp_confirmation_respects_the_minimum_gap():
    with patch.object(copy_service, "TP_CONFIRM_GAP_SECONDS", 15.0):
        assert copy_service._tp_breach_confirmed(5, True, now=1000.0) is False
        assert copy_service._tp_breach_confirmed(5, True, now=1010.0) is False  # 10s < 15s
        assert copy_service._tp_breach_confirmed(5, True, now=1016.0) is True


def test_confirmed_marker_survives_a_failed_flatten_for_the_retry():
    with patch.object(copy_service, "TP_CONFIRM_GAP_SECONDS", 0.0):
        copy_service._tp_breach_confirmed(5, True, now=1000.0)
        assert copy_service._tp_breach_confirmed(5, True, now=1030.0) is True
        # Marker not popped on confirmation -> the retry poll re-fires at once.
        assert copy_service._tp_breach_confirmed(5, True, now=1060.0) is True


def test_swept_close_fee_lands_in_the_same_poll_rail_basis():
    """AUDIT F3: a close booked by the sweep adds its pnl to the rail's
    numerator in the SAME poll — its fee must be subtracted in that poll too,
    or net is overstated by exactly one close's fee."""
    async def _case():
        mirror = _mirror(cumulative_pnl=0.0, cumulative_take_profit_pct=0.0)
        judged = {}

        def _spy_rail(net, alloc, sl, tp):
            judged.setdefault("net", net)
            return None

        client = MagicMock()
        client.get_all_positions.return_value = []

        async def _sweep(*_args, **_kwargs):
            return 10.0, 0.05  # pnl booked, fee booked — both this poll

        with patch.object(copy_service, "run_blocking", side_effect=_inline), \
             patch.object(copy_service, "get_open_copy_positions", return_value=[]), \
             patch.object(copy_service, "get_user_nado_client", return_value=client), \
             patch.object(copy_service, "_sweep_external_closes", side_effect=_sweep), \
             patch.object(copy_service, "set_mirror_unrealized"), \
             patch.object(copy_service, "_rail_decision", side_effect=_spy_rail), \
             patch.object(copy_service, "_notify_user", new_callable=AsyncMock):
            await copy_service._sync_mirror_positions(mirror, {})

        assert judged["net"] == pytest.approx(10.0 - 0.05)

    asyncio.run(_case())


def test_pause_reply_discloses_unmonitored_open_positions():
    """AUDIT F1 disclosure: pausing with open copied positions must say the
    rail and leader-close mirroring are off while paused."""
    mirror = {"id": 5, "user_id": 4242, "active": True, "paused": False,
              "stop_requested": False}
    with patch.object(copy_service, "get_copy_mirror", return_value=mirror), \
         patch.object(copy_service, "pause_copy_mirror"), \
         patch.object(copy_service, "get_open_copy_positions",
                      return_value=[{"id": 1}, {"id": 2}]):
        ok, message = copy_service.pause_copy(4242, 5)

    assert ok
    assert "2 open copied position(s)" in message
    assert "NOT" in message and "stop-loss" in message
