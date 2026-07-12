"""Desk runner: session lifecycle, spot-hours gating, notification coverage.

The runner is the only piece that touches RUNTIME / trading clients, so these
tests patch those boundaries and pin the behaviours that would otherwise only
surface in production:
- every event the controller can emit has a user-facing template (a missing
  one = a silent plan);
- spot market-hours lookup fails OPEN (a broken feed must not freeze trading);
- sessions are torn down for users whose plans all finished.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

from _stubs import install_test_stubs

install_test_stubs()

import inspect

from src.nadobro.engine.controllers import desk as desk_ctrl
from src.nadobro.trading import desk_runtime


def test_every_emitted_event_has_a_notification_template():
    """Cross-check: the controller's _emit('type', ...) literals must all
    appear as keys in _EVENT_TEXT, or that plan silently notifies nothing."""
    src = inspect.getsource(desk_ctrl)
    import re

    emitted = set(re.findall(r'self\._emit\(\s*"([a-z_]+)"', src))
    templates = set(desk_runtime._EVENT_TEXT)
    missing = emitted - templates
    assert not missing, f"emitted desk events with no notification template: {missing}"


def test_spot_market_open_fails_open(monkeypatch):
    from src.nadobro.venue import product_catalog as pc

    def boom(**kw):
        raise RuntimeError("catalog down")

    monkeypatch.setattr(pc, "get_spot_catalog", boom)
    desk_runtime._spot_open_cache.clear()
    # a broken hours feed must not halt trading
    assert desk_runtime._spot_market_open_sync("QQQX", "mainnet") is True


def test_spot_market_closed_detected(monkeypatch):
    from src.nadobro.venue import product_catalog as pc

    monkeypatch.setattr(pc, "get_spot_catalog", lambda **kw: {
        "spots": {"QQQX": {"trading_status": "halted"}}
    })
    desk_runtime._spot_open_cache.clear()
    assert desk_runtime._spot_market_open_sync("QQQX", "mainnet") is False


def test_spot_market_hours_flag(monkeypatch):
    from src.nadobro.venue import product_catalog as pc

    monkeypatch.setattr(pc, "get_spot_catalog", lambda **kw: {
        "spots": {"QQQX": {"trading_status": "live", "market_hours": {"is_open": False}}}
    })
    desk_runtime._spot_open_cache.clear()
    assert desk_runtime._spot_market_open_sync("QQQX", "mainnet") is False


def test_crypto_spot_with_no_hours_is_open(monkeypatch):
    from src.nadobro.venue import product_catalog as pc

    monkeypatch.setattr(pc, "get_spot_catalog", lambda **kw: {
        "spots": {"ETH": {"trading_status": "live"}}
    })
    desk_runtime._spot_open_cache.clear()
    assert desk_runtime._spot_market_open_sync("ETH", "mainnet") is True


def test_disabled_runner_is_a_noop(monkeypatch):
    monkeypatch.setenv("NADO_DESK_ENABLE", "false")
    desk_runtime.set_bot_app(object())
    # must return without scanning the DB
    with patch.object(desk_runtime.desk_store, "list_users_with_active_plans") as scan:
        asyncio.run(desk_runtime.tick_desk_runner())
        scan.assert_not_called()


def test_tick_starts_active_and_tears_down_stale(monkeypatch):
    monkeypatch.setenv("NADO_DESK_ENABLE", "true")
    desk_runtime.set_bot_app(object())
    desk_runtime._RUNNING.clear()
    desk_runtime._boot_standdown_done = True   # steady state (boot pass already ran)
    desk_runtime._RUNNING.add((999, "mainnet"))  # a stale session, no active plans

    scans = {"mainnet": [42], "testnet": []}
    monkeypatch.setattr(desk_runtime.desk_store, "list_users_with_active_plans",
                        lambda network: scans.get(network, []))

    started, stopped, ticked = [], [], []

    async def fake_ensure(uid, network):
        started.append((uid, network))
        return True

    async def fake_stop(uid, network):
        stopped.append((uid, network))

    class FakeRuntime:
        _controllers = {}

        async def tick(self, uid, network, strat):
            ticked.append((uid, network, strat))

    monkeypatch.setattr(desk_runtime, "_ensure_session", fake_ensure)
    monkeypatch.setattr(desk_runtime, "_stop_session", fake_stop)
    monkeypatch.setattr("src.nadobro.strategy.engine_runtime.RUNTIME", FakeRuntime())

    asyncio.run(desk_runtime.tick_desk_runner())

    assert (42, "mainnet") in started      # user with active plans got a session
    assert (999, "mainnet") in stopped     # the stale session was torn down
    assert (42, "mainnet", "desk") in ticked


def _mk_record(plan_id: str, status: str, summary: str = "buy 0.03 BTC"):
    class _Plan:
        def __init__(self, pid):
            self.plan_id = pid

        def describe(self):
            return summary

    return {"plan": _Plan(plan_id), "plan_id": plan_id, "status": status, "state": {}}


def test_redeploy_stands_down_active_plans_and_notifies(monkeypatch):
    """Redeploy contract: nothing trades unless the user starts it. The first
    tick after boot parks every still-active plan (cancelled, fills kept),
    notifies the owner what was NOT resumed, and starts no session."""
    monkeypatch.setenv("NADO_DESK_ENABLE", "true")
    monkeypatch.delenv("NADO_DESK_RESUME_ON_RESTART", raising=False)
    desk_runtime.set_bot_app(object())
    desk_runtime._RUNNING.clear()
    desk_runtime._boot_standdown_done = False

    scans = {"mainnet": [7], "testnet": []}
    monkeypatch.setattr(desk_runtime.desk_store, "list_users_with_active_plans",
                        lambda network: scans.get(network, []))
    monkeypatch.setattr(desk_runtime.desk_store, "list_active_plans",
                        lambda uid, network: [_mk_record("p-wait", "awaiting_trigger"),
                                              _mk_record("p-run", "running")])
    finished = []
    monkeypatch.setattr(
        desk_runtime.desk_store, "finish_plan",
        lambda pid, network, status, error=None: finished.append(
            (pid, network, status, error)) or True,
    )
    notified = []

    async def fake_notify(uid, evt):
        notified.append((uid, evt["type"]))

    monkeypatch.setattr(desk_runtime, "_notify_event", fake_notify)

    ensured = []

    async def fake_ensure(uid, network):
        ensured.append((uid, network))
        return True

    monkeypatch.setattr(desk_runtime, "_ensure_session", fake_ensure)

    asyncio.run(desk_runtime.tick_desk_runner())

    assert [(f[0], f[2]) for f in finished] == [
        ("p-wait", "cancelled"), ("p-run", "cancelled")]
    assert all("user-initiated" in (f[3] or "") for f in finished)
    # Wording differs by prior status: a waiting trigger vs an open position.
    assert (7, "plan_parked_waiting") in notified
    assert (7, "plan_parked_running") in notified
    assert ensured == []                       # no session before a clean slate
    assert desk_runtime._boot_standdown_done is True

    # Steady state: the parked plans are gone; nothing resumes on later ticks.
    scans["mainnet"] = []
    asyncio.run(desk_runtime.tick_desk_runner())
    assert ensured == []


def test_resume_env_flag_restores_legacy_resume(monkeypatch):
    monkeypatch.setenv("NADO_DESK_ENABLE", "true")
    monkeypatch.setenv("NADO_DESK_RESUME_ON_RESTART", "1")
    desk_runtime.set_bot_app(object())
    desk_runtime._RUNNING.clear()
    desk_runtime._boot_standdown_done = False

    scans = {"mainnet": [42], "testnet": []}
    monkeypatch.setattr(desk_runtime.desk_store, "list_users_with_active_plans",
                        lambda network: scans.get(network, []))
    parked = []
    monkeypatch.setattr(desk_runtime.desk_store, "finish_plan",
                        lambda *a, **k: parked.append(a) or True)

    started, ticked = [], []

    async def fake_ensure(uid, network):
        started.append((uid, network))
        return True

    class FakeRuntime:
        _controllers = {}

        async def tick(self, uid, network, strat):
            ticked.append((uid, network, strat))

    monkeypatch.setattr(desk_runtime, "_ensure_session", fake_ensure)
    monkeypatch.setattr("src.nadobro.strategy.engine_runtime.RUNTIME", FakeRuntime())

    asyncio.run(desk_runtime.tick_desk_runner())

    assert parked == []                        # nothing stood down
    assert (42, "mainnet") in started          # legacy resume behavior


def test_standdown_db_failure_blocks_sessions_until_complete(monkeypatch):
    """An incomplete stand-down must not let sessions start — an unparked plan
    would auto-resume after all. Retry next tick."""
    monkeypatch.setenv("NADO_DESK_ENABLE", "true")
    monkeypatch.delenv("NADO_DESK_RESUME_ON_RESTART", raising=False)
    desk_runtime.set_bot_app(object())
    desk_runtime._RUNNING.clear()
    desk_runtime._boot_standdown_done = False

    calls = {"n": 0}

    def flaky_scan(network):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("db down")
        return []

    monkeypatch.setattr(desk_runtime.desk_store, "list_users_with_active_plans", flaky_scan)

    ensured = []

    async def fake_ensure(uid, network):
        ensured.append((uid, network))
        return True

    monkeypatch.setattr(desk_runtime, "_ensure_session", fake_ensure)

    asyncio.run(desk_runtime.tick_desk_runner())
    assert desk_runtime._boot_standdown_done is False   # retrying
    assert ensured == []

    asyncio.run(desk_runtime.tick_desk_runner())        # DB back: completes
    assert desk_runtime._boot_standdown_done is True
    assert ensured == []                                # nothing was active


def test_failed_controller_is_rebuilt_not_ticked_dead(monkeypatch):
    """_ensure_session must NOT short-circuit on a FAILED controller (is_active
    False) — it has to fall through to a rebuild."""
    desk_runtime._RUNNING.clear()
    desk_runtime._RUNNING.add((7, "mainnet"))

    class DeadController:
        is_active = False

    class FakeRuntime:
        _controllers = {(7, "mainnet", "desk"): DeadController()}
        start = AsyncMock()

    fake = FakeRuntime()
    monkeypatch.setattr("src.nadobro.strategy.engine_runtime.RUNTIME", fake)
    monkeypatch.setattr("src.nadobro.strategy.engine_runtime.build_adapter",
                        lambda *a, **k: object())
    monkeypatch.setattr("src.nadobro.strategy.engine_runtime.build_product_meta_from_catalog",
                        lambda *a, **k: {})
    monkeypatch.setattr("src.nadobro.services.user_service.get_user_nado_client",
                        lambda *a, **k: object())
    monkeypatch.setattr("src.nadobro.trading.engine_persistence.DbInventoryRepository",
                        lambda *a, **k: object())

    asyncio.run(desk_runtime._ensure_session(7, "mainnet"))
    fake.start.assert_awaited_once()  # rebuilt, not skipped
