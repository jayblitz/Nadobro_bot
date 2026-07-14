"""Boot stand-down: redeploys never auto-resume copy trading.

``boot_stand_down_mirrors`` pauses every active, unpaused mirror at boot and
notifies each owner with Resume/Stop controls (main.py gates the call on
NADO_COPY_AUTO_RESUME, default false). Paused mirrors are excluded from the
polling query, so nothing mirrors again until the user taps Resume.
"""

import asyncio

import src.nadobro.trading.copy_service as copy_service


def _mirror(mid, user_id, *, network="mainnet", wallet="0xleader00001", label="Leader"):
    return {
        "id": mid,
        "user_id": user_id,
        "trader_id": 1,
        "network": network,
        "wallet_address": wallet,
        "label": label,
    }


def test_stand_down_pauses_all_and_notifies_each_owner(monkeypatch):
    mirrors = [
        _mirror(11, 1001, label="Alpha"),
        _mirror(12, 1001, network="testnet", label="Beta"),
        _mirror(21, 2002, label="Gamma"),
    ]
    paused, notified = [], []

    monkeypatch.setattr(copy_service, "get_all_active_mirrors_v2", lambda *a, **k: mirrors)
    monkeypatch.setattr(copy_service, "pause_copy_mirror", lambda mid: paused.append(mid))
    monkeypatch.setattr(
        copy_service, "get_open_copy_positions",
        lambda mid: [{"id": 1}] if mid == 11 else [],
    )

    async def _capture_notify(user_id, text, reply_markup=None):
        notified.append((user_id, text, reply_markup))

    monkeypatch.setattr(copy_service, "_notify_user", _capture_notify)

    count = asyncio.run(copy_service.boot_stand_down_mirrors())

    assert count == 3
    assert sorted(paused) == [11, 12, 21]
    # One message per owner, not per mirror.
    assert sorted(u for u, _, _ in notified) == [1001, 2002]

    msg_1001 = next(t for u, t, _ in notified if u == 1001)
    assert "paused" in msg_1001.lower()
    assert "Alpha" in msg_1001 and "Beta" in msg_1001
    # Open exposure is called out explicitly (mirror 11 had an open row).
    assert "NOT monitored" in msg_1001

    # The prompt carries the dashboard's Resume/Stop controls.
    markup_1001 = next(m for u, _, m in notified if u == 1001)
    callbacks = [b.callback_data for row in markup_1001.inline_keyboard for b in row]
    assert "copy:resume:11" in callbacks and "copy:stop:11" in callbacks
    assert "copy:resume:12" in callbacks and "copy:stop:12" in callbacks


def test_stand_down_with_no_active_mirrors_is_quiet(monkeypatch):
    monkeypatch.setattr(copy_service, "get_all_active_mirrors_v2", lambda *a, **k: [])
    called = []
    monkeypatch.setattr(copy_service, "pause_copy_mirror", lambda mid: called.append(mid))

    assert asyncio.run(copy_service.boot_stand_down_mirrors()) == 0
    assert called == []


def test_one_bad_row_does_not_block_the_rest(monkeypatch):
    mirrors = [_mirror(31, 3003, label="Ok"), _mirror(32, 3003, label="Bad")]
    paused = []

    def _pause(mid):
        if mid == 32:
            raise RuntimeError("db hiccup")
        paused.append(mid)

    monkeypatch.setattr(copy_service, "get_all_active_mirrors_v2", lambda *a, **k: mirrors)
    monkeypatch.setattr(copy_service, "pause_copy_mirror", _pause)
    monkeypatch.setattr(copy_service, "get_open_copy_positions", lambda mid: [])

    async def _noop_notify(user_id, text, reply_markup=None):
        return None

    monkeypatch.setattr(copy_service, "_notify_user", _noop_notify)

    count = asyncio.run(copy_service.boot_stand_down_mirrors())

    assert paused == [31]
    assert count == 1  # only successfully paused mirrors are counted
