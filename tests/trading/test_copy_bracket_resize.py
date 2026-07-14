"""Force-resize of TP/SL brackets after a mirrored partial close.

The force path re-places reduce-only brackets for the shrunken copy size even
when the leader's prices did not move. With unchanged prices there are no
price columns to UPDATE — the old code built ``UPDATE copy_positions SET
WHERE id = %s`` (empty SET, a SQL syntax error) and crashed the mirror's
sync. These tests pin: no empty-SET statement is ever issued, and the
brackets are still cancelled + re-placed at the new size.
"""

import pytest

import src.nadobro.db as db_mod
import src.nadobro.trading.copy_service as copy_service


class _StubVenueClient:
    def __init__(self):
        self.cancelled = []

    def cancel_order(self, pid, digest):
        self.cancelled.append((pid, digest))


@pytest.fixture()
def spies(monkeypatch):
    executed = []
    placed = []
    client = _StubVenueClient()

    monkeypatch.setattr(db_mod, "execute", lambda sql, params=None: executed.append((sql, params)))
    monkeypatch.setattr(
        copy_service, "_place_tp_sl_orders",
        lambda *a, **k: placed.append(k) or {"tp_order_digest": "0xnew_tp", "sl_order_digest": "0xnew_sl"},
    )
    monkeypatch.setattr(copy_service, "get_user_nado_client", lambda *a, **k: client)
    return executed, placed, client


def _cp(size=0.5):
    return {
        "id": 777,
        "product_id": 2,
        "product_name": "BTC-PERP",
        "side": "long",
        "size": size,
        "leverage": 3.0,
        "tp_price": 70000.0,
        "sl_price": 60000.0,
        "tp_order_digest": "0xold_tp",
        "sl_order_digest": "0xold_sl",
    }


def test_force_resize_with_unchanged_prices_issues_no_empty_set(spies):
    executed, placed, client = spies
    leader = {"tp_price": 70000.0, "sl_price": 60000.0}  # unchanged

    copy_service._update_tp_sl_if_changed(_cp(size=0.25), leader, user_id=1, network="mainnet", force=True)

    # No malformed statement anywhere.
    assert all("SET  WHERE" not in sql for sql, _ in executed)
    # The only UPDATE is the digest refresh after re-placing.
    assert len(executed) == 1
    assert "tp_order_digest" in executed[0][0]
    # Old brackets cancelled, new ones placed for the reduced size.
    assert {d for _, d in client.cancelled} == {"0xold_tp", "0xold_sl"}
    assert len(placed) == 1
    assert placed[0]["size"] == pytest.approx(0.25)
    assert placed[0]["tp_price"] == pytest.approx(70000.0)
    assert placed[0]["sl_price"] == pytest.approx(60000.0)


def test_no_force_and_unchanged_prices_is_a_no_op(spies):
    executed, placed, client = spies
    leader = {"tp_price": 70000.0, "sl_price": 60000.0}

    copy_service._update_tp_sl_if_changed(_cp(), leader, user_id=1, network="mainnet", force=False)

    assert executed == [] and placed == [] and client.cancelled == []


def test_changed_price_updates_row_and_replaces(spies):
    executed, placed, client = spies
    leader = {"tp_price": 72000.0, "sl_price": 60000.0}  # TP moved

    copy_service._update_tp_sl_if_changed(_cp(), leader, user_id=1, network="mainnet", force=False)

    price_updates = [sql for sql, _ in executed if "tp_price = %s" in sql]
    assert len(price_updates) == 1
    assert all("SET  WHERE" not in sql for sql, _ in executed)
    assert len(placed) == 1 and placed[0]["tp_price"] == pytest.approx(72000.0)


def test_force_with_no_brackets_at_all_is_a_no_op(spies):
    executed, placed, client = spies
    cp = _cp()
    cp.update({"tp_price": None, "sl_price": None, "tp_order_digest": None, "sl_order_digest": None})

    copy_service._update_tp_sl_if_changed(cp, {"tp_price": None, "sl_price": None}, user_id=1, network="mainnet", force=True)

    assert executed == [] and placed == []
