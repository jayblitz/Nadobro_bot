"""CLICK-PATH-BLOCKING regression: the inline wallet:view / wallet:balance
button handlers must route their venue reads through run_blocking_sdk_capped
(SDK pool + a short wall-clock cap) and NEVER call the venue bare on the event
loop — the documented "sync get_balance hung taps 30-60s" incident, which had
re-manifested on the inline-button path the earlier home-card fix never reached.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.handlers import wallet_handler


def test_wallet_balance_offloads_both_venue_reads_through_the_cap():
    async def body():
        client = MagicMock()
        # run_blocking_sdk_capped is awaited twice: client construction, then
        # get_balance — both off the loop, both capped.
        capped = AsyncMock(side_effect=[client, {"balances": {0: 123.0}}])
        with patch.object(wallet_handler, "run_blocking_sdk_capped", capped), \
             patch.object(wallet_handler, "_edit_loc", new_callable=AsyncMock) as edit:
            await wallet_handler._handle_wallet(MagicMock(), "wallet:balance", 42, None)
        assert capped.await_count == 2
        # get_balance was passed as a REFERENCE to the cap, never called bare.
        client.get_balance.assert_not_called()
        edit.assert_awaited()

    asyncio.run(body())


def test_wallet_balance_timeout_shows_refreshing_placeholder():
    async def body():
        client = MagicMock()
        capped = AsyncMock(side_effect=[client, wallet_handler._TIMED_OUT])
        with patch.object(wallet_handler, "run_blocking_sdk_capped", capped), \
             patch.object(wallet_handler, "_edit_loc", new_callable=AsyncMock) as edit:
            await wallet_handler._handle_wallet(MagicMock(), "wallet:balance", 42, None)
        # Placeholder instead of a hang; no exception.
        assert "Refreshing balance" in edit.await_args.args[1]

    asyncio.run(body())


def test_wallet_view_offloads_signer_read_and_times_out_gracefully():
    async def body():
        # The verify_signer venue read goes through the cap; on timeout the tap
        # returns a refreshing placeholder instead of freezing the loop.
        capped = AsyncMock(return_value=wallet_handler._TIMED_OUT)
        with patch.object(wallet_handler, "run_blocking_sdk_capped", capped), \
             patch.object(wallet_handler, "_edit_loc", new_callable=AsyncMock) as edit:
            await wallet_handler._handle_wallet(MagicMock(), "wallet:view", 42, None)
        capped.assert_awaited_once()
        assert "Refreshing wallet" in edit.await_args.args[1]

    asyncio.run(body())


def test_wallet_view_renders_linked_info_when_read_succeeds():
    async def body():
        capped = AsyncMock(return_value={"is_linked": True, "address": "0xabc"})
        with patch.object(wallet_handler, "run_blocking_sdk_capped", capped), \
             patch.object(wallet_handler, "fmt_wallet_info", return_value="INFO"), \
             patch.object(wallet_handler, "_edit_loc", new_callable=AsyncMock) as edit:
            await wallet_handler._handle_wallet(MagicMock(), "wallet:view", 42, None)
        assert edit.await_args.args[1] == "INFO"

    asyncio.run(body())
