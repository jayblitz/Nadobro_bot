import asyncio
from datetime import datetime, timezone

from src.nadobro.services.time_limit_watcher import time_limit_tick


def test_time_limit_tick_enqueues_claimed_rows(monkeypatch):
    claimed = {"positions": [{"id": 1}], "orders": [{"id": 2}]}
    monkeypatch.setattr("src.nadobro.services.time_limit_watcher.time_limit_enabled", lambda: True)
    monkeypatch.setattr("src.nadobro.services.time_limit_watcher.fetch_due_time_limits", lambda now, network: claimed)
    enqueued = []

    async def fake_enqueue(payload, dedupe_key):
        enqueued.append((payload, dedupe_key))
        return True

    monkeypatch.setattr("src.nadobro.services.time_limit_watcher.enqueue_strategy", fake_enqueue)
    asyncio.run(time_limit_tick())
    assert any("time-limit-position" in key for _payload, key in enqueued)
    assert any("time-limit-order" in key for _payload, key in enqueued)
