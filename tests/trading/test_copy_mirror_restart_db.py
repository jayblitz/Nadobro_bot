"""Restart semantics of copy mirrors (real Postgres; auto-skips without one).

``UNIQUE(user_id, trader_id, network)`` routes every stop→restart of the same
trader through ``create_copy_mirror_v2``'s ON CONFLICT upsert. A restart MUST
zero the whole accounting spine: keeping the previous run's
``cumulative_fees_usd`` started the new run at net = -fees, which could trip
the cumulative stop-loss rail with zero trades in the new run.
"""

import os

import pytest


def _db_reachable() -> bool:
    if not (os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DATABASE_URL")):
        return False
    try:
        import psycopg2

        url = os.environ.get("SUPABASE_DATABASE_URL") or os.environ["DATABASE_URL"]
        psycopg2.connect(url).close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _db_reachable(), reason="no reachable Postgres (DATABASE_URL)")

_USER = 990_017_001
_WALLET = "0xtest_restart_reset_leader"


@pytest.fixture()
def trader_id():
    from src.nadobro.db import execute
    from src.nadobro.models.database import upsert_copy_trader

    execute("DELETE FROM copy_positions WHERE user_id = %s", (_USER,))
    execute("DELETE FROM copy_mirrors WHERE user_id = %s", (_USER,))
    execute("DELETE FROM copy_traders WHERE wallet_address = %s", (_WALLET,))
    tid = upsert_copy_trader(_WALLET, label="restart-reset", is_curated=True)
    yield tid
    execute("DELETE FROM copy_positions WHERE user_id = %s", (_USER,))
    execute("DELETE FROM copy_mirrors WHERE user_id = %s", (_USER,))
    execute("DELETE FROM copy_traders WHERE id = %s", (tid,))


def test_restart_resets_accounting_and_session(trader_id):
    from src.nadobro.db import execute, query_one
    from src.nadobro.models.database import (
        create_copy_mirror_v2,
        stop_copy_mirror,
        update_mirror_accounting,
    )
    from src.nadobro.trading.copy_service import _rail_decision

    kwargs = dict(
        user_id=_USER, trader_id=trader_id, network="mainnet",
        margin_per_trade=100.0, max_leverage=5.0,
        cumulative_stop_loss_pct=10.0, cumulative_take_profit_pct=0.0,
        total_allocated_usd=500.0,
    )

    # Run 1: trade, pay fees, stop.
    mirror_id = create_copy_mirror_v2(**kwargs)
    update_mirror_accounting(mirror_id, pnl_delta=-20.0, fees_delta=60.0, volume_delta=4000.0)
    execute(
        "UPDATE copy_mirrors SET last_unrealized_pnl_usd = -33.0, strategy_session_id = 424242 WHERE id = %s",
        (mirror_id,),
    )
    stop_copy_mirror(mirror_id)

    # Run 2: restart the same trader — same row via ON CONFLICT.
    mirror_id2 = create_copy_mirror_v2(**kwargs)
    assert mirror_id2 == mirror_id

    row = query_one("SELECT * FROM copy_mirrors WHERE id = %s", (mirror_id2,))
    assert row["active"] is True and row["paused"] is False
    assert float(row["cumulative_pnl"]) == 0.0
    assert float(row["cumulative_fees_usd"]) == 0.0
    assert float(row["cumulative_volume_usd"]) == 0.0
    assert float(row["last_unrealized_pnl_usd"]) == 0.0
    assert row["strategy_session_id"] is None
    assert row["stopped_at"] is None and row["auto_stopped_reason"] is None

    # The first poll of the restarted mirror judges net = 0 + 0 - fees(=0):
    # the rail must NOT fire on a fresh run.
    net = float(row["cumulative_pnl"]) + 0.0 - float(row["cumulative_fees_usd"])
    assert _rail_decision(net, float(row["total_allocated_usd"]), 10.0, 0.0) is None
