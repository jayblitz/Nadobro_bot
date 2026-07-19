"""COPY-SWEEP-CONFIRM: unverified external closes need two consecutive polls,
and no sweep runs at all while archive-backed venue reads are unreliable."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.nadobro.trading import copy_service


@pytest.fixture(autouse=True)
def _clean_runtime_state():
    copy_service._EXTERNAL_CLOSE_SUSPECTS.clear()
    yield
    copy_service._EXTERNAL_CLOSE_SUSPECTS.clear()


async def _inline(fn, *args, **kwargs):
    return fn(*args, **kwargs)


MIRROR = {"id": 5, "user_id": 4242, "network": "mainnet"}


def _open_row(pid=1):
    return {
        "id": 77, "product_id": pid, "product_name": "BTC-PERP", "side": "long",
        "entry_price": 100.0, "size": 1.0, "leverage": 3.0,
        "tp_order_digest": None, "sl_order_digest": None,
    }


def _client(mid=105.0):
    client = MagicMock()
    client.get_market_price.return_value = {"bid": mid - 0.1, "ask": mid + 0.1, "mid": mid}
    return client


def _sweep(copy_map, venue_map, client, bracket=(0.0, 0.0, "external_close"),
           unreliable=False):
    async def _case():
        with patch.object(copy_service, "run_blocking", side_effect=_inline), \
             patch.object(copy_service, "_archive_reads_unreliable", return_value=unreliable), \
             patch.object(copy_service, "_resolve_bracket_fill", return_value=bracket), \
             patch.object(copy_service, "update_mirror_accounting") as accounting, \
             patch.object(copy_service, "close_copy_position") as close_row, \
             patch.object(copy_service, "reduce_copy_position"), \
             patch.object(copy_service, "_notify_user", new_callable=AsyncMock):
            pnl, fees = await copy_service._sweep_external_closes(
                MIRROR, copy_map, venue_map, client
            )
        return pnl, fees, accounting, close_row

    return asyncio.run(_case())


def test_unverified_external_close_waits_for_a_second_poll():
    row = _open_row()
    with patch.object(copy_service, "EXTERNAL_CLOSE_CONFIRM_SECONDS", 0.0):
        # Poll 1: position missing from the venue read, no bracket evidence —
        # suspicion only, nothing booked, the row stays tracked.
        pnl, fees, accounting, close_row = _sweep({1: row}, {}, _client())
        assert (pnl, fees) == (0.0, 0.0)
        accounting.assert_not_called()
        close_row.assert_not_called()
        assert (5, 1) in copy_service._EXTERNAL_CLOSE_SUSPECTS

        # Poll 2: still missing — now it books at mid.
        pnl2, _fees2, accounting2, close_row2 = _sweep({1: dict(row)}, {}, _client(mid=105.0))
        assert pnl2 == pytest.approx(5.0)  # (105 - 100) * 1.0 long
        accounting2.assert_called_once()
        close_row2.assert_called_once()
        assert (5, 1) not in copy_service._EXTERNAL_CLOSE_SUSPECTS


def test_position_reappearing_clears_the_suspicion():
    row = _open_row()
    _sweep({1: row}, {}, _client())
    assert (5, 1) in copy_service._EXTERNAL_CLOSE_SUSPECTS

    venue_map = {1: {"product_id": 1, "amount": 1.0, "side": "LONG"}}
    pnl, _fees, accounting, close_row = _sweep({1: dict(row)}, venue_map, _client())
    assert pnl == 0.0
    accounting.assert_not_called()
    close_row.assert_not_called()
    assert (5, 1) not in copy_service._EXTERNAL_CLOSE_SUSPECTS


def test_bracket_confirmed_close_books_on_the_first_poll():
    row = _open_row()
    row["tp_order_digest"] = "0xtp"
    pnl, fees, accounting, close_row = _sweep(
        {1: row}, {}, _client(), bracket=(110.0, 0.05, "bracket_take_profit"),
    )
    assert pnl == pytest.approx(10.0)
    assert fees == pytest.approx(0.05)
    accounting.assert_called_once()
    close_row.assert_called_once()
    assert close_row.call_args.kwargs.get("reason") == "bracket_take_profit"


def test_sweep_is_skipped_entirely_while_archive_reads_are_unreliable():
    # Isolated-subaccount discovery silently returns [] under archive
    # rate-limits, vanishing every isolated position from the venue read —
    # sweeping then would fabricate closes and let the open loop duplicate.
    row = _open_row()
    pnl, fees, accounting, close_row = _sweep({1: row}, {}, _client(), unreliable=True)
    assert (pnl, fees) == (0.0, 0.0)
    accounting.assert_not_called()
    close_row.assert_not_called()
    assert copy_service._EXTERNAL_CLOSE_SUSPECTS == {}
