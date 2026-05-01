import asyncio

from src.nadobro.services.condition_watcher import condition_tick


def test_condition_tick_enqueues_armed_orders(monkeypatch):
    monkeypatch.setattr("src.nadobro.services.condition_watcher.studio_enabled", lambda: True)
    monkeypatch.setattr("src.nadobro.services.condition_watcher.get_armed_conditional_orders", lambda network: [{"id": 7}])
    enqueued = []

    async def fake_enqueue(payload, dedupe_key):
        enqueued.append((payload, dedupe_key))
        return True

    monkeypatch.setattr("src.nadobro.services.condition_watcher.enqueue_strategy", fake_enqueue)
    asyncio.run(condition_tick())
    assert any(payload["kind"] == "condition_order" for payload, _key in enqueued)
