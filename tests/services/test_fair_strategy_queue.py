"""Tests for deficit round-robin strategy queue fairness."""
from __future__ import annotations

import asyncio

from src.nadobro.trading.execution_queue import _FairStrategyQueue


async def _alternates_between_users() -> None:
    q = _FairStrategyQueue(maxsize=10)
    await q.put({"telegram_id": 1, "network": "mainnet"})
    await q.put({"telegram_id": 2, "network": "mainnet"})
    await q.put({"telegram_id": 1, "network": "mainnet"})
    await q.put({"telegram_id": 2, "network": "mainnet"})

    first = await q.get()
    second = await q.get()
    assert {first["telegram_id"], second["telegram_id"]} == {1, 2}

    third = await q.get()
    fourth = await q.get()
    assert {third["telegram_id"], fourth["telegram_id"]} == {1, 2}


def test_drr_alternates_between_users():
    asyncio.run(_alternates_between_users())
