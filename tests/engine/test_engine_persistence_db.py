"""Integration tests for the DB-backed engine persistence (C1/C2/C4).

These run against a REAL Postgres (no mocks) — they auto-skip when no database
is reachable, so CI without a DB is unaffected. Locally:

    DATABASE_URL=postgresql://postgres@127.0.0.1:55432/nadotest \
        PYTHONPATH=tests:. .venv/bin/python -m pytest tests/engine/test_engine_persistence_db.py
"""
from __future__ import annotations

import os
import pathlib
from decimal import Decimal

import pytest

from tests.engine._mock_nado import MockNadoAdapter

MIGRATIONS = pathlib.Path("src/nadobro/migrations")


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


@pytest.fixture(scope="module", autouse=True)
def _schema():
    from src.nadobro.db import execute

    for mig in ("0007_engine_v2_tables.sql", "0009_engine_kill_switch.sql",
                "0013_engine_controller_state.sql"):
        execute((MIGRATIONS / mig).read_text())
    # clean slate for deterministic assertions
    execute("TRUNCATE engine_position_hold, engine_executors, engine_kill_switch, "
            "engine_controller_state")
    yield


def test_db_inventory_condor_example_persists_and_reads_back():
    from src.nadobro.engine.types import TradeType
    from src.nadobro.services.engine_persistence import DbInventoryRepository

    repo = DbInventoryRepository()
    uid, pair, cid = 9001, "SOL-USDC", "grid-x"
    repo.apply_fill(uid, pair, cid, TradeType.BUY, Decimal(100), Decimal(15000))   # @150
    repo.apply_fill(uid, pair, cid, TradeType.BUY, Decimal(50), Decimal(7250))     # @145
    repo.apply_fill(uid, pair, cid, TradeType.SELL, Decimal(100), Decimal(15500))  # @155

    # read back through a *fresh* repo instance -> proves it is in Postgres
    hold = DbInventoryRepository().get(uid, pair, cid)
    assert round(hold.realized_pnl, 2) == Decimal("666.67")   # (155 - 148.33..) * 100
    assert hold.net_amount_base == Decimal(50)
    assert hold.breakeven is not None and round(hold.breakeven, 2) == Decimal("148.33")
    assert [h.controller_id for h in DbInventoryRepository().list_for_user(uid)] == [cid]


def test_controller_progress_roundtrip_and_clear():
    from decimal import Decimal
    from src.nadobro.services.engine_persistence import (
        upsert_controller_progress, get_controller_progress, clear_controller_progress,
    )

    cid = "delta_neutral-abc123"
    upsert_controller_progress(
        cid, 9100, strategy="dn", network="testnet",
        cycles_completed=2, funding_earned_usd=Decimal("1.25"), phase="HOLDING",
    )
    row = get_controller_progress(cid)
    assert row is not None
    assert int(row["cycles_completed"]) == 2
    assert Decimal(str(row["funding_earned_usd"])) == Decimal("1.25")
    assert row["phase"] == "HOLDING"

    # Idempotent upsert overwrites (not accumulates) — it mirrors live state.
    upsert_controller_progress(
        cid, 9100, strategy="dn", network="testnet",
        cycles_completed=5, funding_earned_usd=Decimal("3.50"), phase="WAITING",
    )
    row2 = get_controller_progress(cid)
    assert int(row2["cycles_completed"]) == 5
    assert Decimal(str(row2["funding_earned_usd"])) == Decimal("3.50")

    clear_controller_progress(cid)
    assert get_controller_progress(cid) is None


def test_db_executor_store_persists_lifecycle():
    import asyncio

    from src.nadobro.engine.executors.order_executor import OrderExecutor, OrderExecutorConfig
    from src.nadobro.engine.types import CloseType, ExecutionStrategy, TradeType
    from src.nadobro.services.engine_persistence import DbExecutorStore

    adapter = MockNadoAdapter(mid=Decimal(100))
    cfg = OrderExecutorConfig("SOL-USDC", TradeType.BUY, Decimal(1), ExecutionStrategy.MARKET)
    ex = OrderExecutor(cfg, user_id=9002, controller_id="mm-1", adapter=adapter)
    asyncio.run(ex.on_create())  # market fill -> terminates COMPLETED

    store = DbExecutorStore()
    store.save(ex)
    row = store.get(ex.id)
    assert row is not None
    assert row["user_id"] == 9002 and row["controller_id"] == "mm-1"
    assert row["strategy_type"] == "order" and row["state"] == "TERMINATED"
    assert row["close_type"] == CloseType.COMPLETED.value
    # idempotent upsert
    store.save(ex)
    assert store.get(ex.id)["state"] == "TERMINATED"


def test_db_kill_switch_persists_across_instances():
    from src.nadobro.services.engine_persistence import DbKillSwitchStore

    DbKillSwitchStore().engage("drawdown breach")
    fresh = DbKillSwitchStore()
    assert fresh.is_engaged() is True
    assert fresh.reason() == "drawdown breach"
    DbKillSwitchStore().disengage()
    assert DbKillSwitchStore().is_engaged() is False


def test_count_engine_orders_scoped_per_run():
    """Order/executor counts must be scoped to the run's session id — a stable
    controller_id is reused across runs, so an unscoped count would sum every
    past run of the same strategy into the current one."""
    import uuid
    from src.nadobro.db import execute
    from src.nadobro.services.engine_persistence import count_engine_orders

    cid = "grid:777:mainnet"
    # Run A (session 101): 2 executors, one closed -> 3 placed.
    # Run B (session 202): 1 executor -> 1 placed.
    execute(
        "INSERT INTO engine_executors (id,user_id,controller_id,strategy_type,trading_pair,side,"
        "config_json,state,volume_quote,keep_position,created_at,terminated_at,strategy_session_id) "
        "VALUES (%s,777,%s,'grid','BTC-PERP','BUY','{}'::jsonb,'ACTIVE',1,false,now(),NULL,101)",
        (str(uuid.uuid4()), cid),
    )
    execute(
        "INSERT INTO engine_executors (id,user_id,controller_id,strategy_type,trading_pair,side,"
        "config_json,state,volume_quote,keep_position,created_at,terminated_at,strategy_session_id) "
        "VALUES (%s,777,%s,'grid','BTC-PERP','BUY','{}'::jsonb,'TERMINATED',1,false,now(),now(),101)",
        (str(uuid.uuid4()), cid),
    )
    execute(
        "INSERT INTO engine_executors (id,user_id,controller_id,strategy_type,trading_pair,side,"
        "config_json,state,volume_quote,keep_position,created_at,terminated_at,strategy_session_id) "
        "VALUES (%s,777,%s,'grid','BTC-PERP','BUY','{}'::jsonb,'ACTIVE',1,false,now(),NULL,202)",
        (str(uuid.uuid4()), cid),
    )

    run_a = count_engine_orders(cid, 101)
    run_b = count_engine_orders(cid, 202)
    both = count_engine_orders(cid)
    # Run A: 2 executors + 1 closed = 3 placed; Run B: 1 executor = 1 placed.
    assert run_a["orders_placed"] == 3
    assert run_b["orders_placed"] == 1
    assert both["orders_placed"] == 4  # unscoped sums both runs


def test_remote_active_scoped_to_run_ignores_stale_rows():
    """A prior run's non-terminated engine_executors row must NOT make
    is_running()/_remote_active report the strategy as running for a NEW run —
    that is the permanent stale-row trap that blocked order placement."""
    import uuid
    from src.nadobro.db import execute
    from src.nadobro.services.engine_runtime import _remote_active
    from src.nadobro.services.engine_persistence import terminate_engine_executors

    cid = "dgrid:888:mainnet"
    # Prior run (session 1) left an ACTIVE row behind.
    execute(
        "INSERT INTO engine_executors (id,user_id,controller_id,strategy_type,trading_pair,side,"
        "config_json,state,volume_quote,keep_position,created_at,strategy_session_id) "
        "VALUES (%s,888,%s,'grid','BTC-PERP','BUY','{}'::jsonb,'ACTIVE',0,false,now(),1)",
        (str(uuid.uuid4()), cid),
    )
    # New run = session 2: must NOT see the stale session-1 row.
    assert _remote_active("dgrid", 888, "mainnet", session_id=2) is False
    # Same run = session 1: sees it.
    assert _remote_active("dgrid", 888, "mainnet", session_id=1) is True
    # Unscoped (back-compat): sees any non-terminated row.
    assert _remote_active("dgrid", 888, "mainnet") is True
    # Cross-process stop sweep clears it.
    terminate_engine_executors(cid)
    assert _remote_active("dgrid", 888, "mainnet") is False


# ---------------------------------------------------------------------------
# Per-session PnL integrity (false-SL bug, session #40)
# ---------------------------------------------------------------------------
# get_session_live_metrics must be STRICT by strategy_session_id: a run's
# realized/volume/fees/net_base/signed_cash come ONLY from its own tagged fills,
# never an overlapping run or an untagged (account-only) match on the product.

_TRADES_DDL = """
CREATE TABLE IF NOT EXISTS trades_mainnet (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT,
  product_id INTEGER,
  side TEXT,
  status TEXT,
  source TEXT,
  size NUMERIC,
  price NUMERIC,
  fill_size NUMERIC,
  fill_price NUMERIC,
  fill_fee NUMERIC,
  fees NUMERIC,
  builder_fee NUMERIC,
  submission_idx BIGINT,
  realized_pnl_x18 NUMERIC(78,0),
  fee_x18 NUMERIC(78,0),
  base_filled_x18 NUMERIC(78,0),
  quote_filled_x18 NUMERIC(78,0),
  strategy_session_id BIGINT
);
"""


def _insert_fill(user_id, sid, side, base, price, *, fee=0.0, product_id=2):
    """Insert a recorder-style fill (no x18 venue columns yet) for a session.
    Mirrors DbTradeRecorder's NOT NULL columns (product_name/order_type/size)."""
    from src.nadobro.db import execute
    execute(
        "INSERT INTO trades_mainnet (user_id,product_id,product_name,order_type,side,status,source,"
        "size,price,fill_size,fill_price,fill_fee,strategy_session_id) "
        "VALUES (%s,%s,'BTC-PERP','match',%s,'filled','strategy',%s,%s,%s,%s,%s,%s)",
        (user_id, product_id, side, base, price, base, price, fee, sid),
    )


def test_session_realized_is_flat_aware():
    """realized must be the venue per-match PnL, and the recorder cash-flow
    fallback may ONLY be used when the run is flat. An OPEN position must report
    realized 0 (NOT the raw cash spent) — the bogus -$506 bug."""
    from src.nadobro.db import execute
    from src.nadobro.models.database import get_session_live_metrics

    execute(_TRADES_DDL)
    execute("DELETE FROM trades_mainnet WHERE user_id = 4044")
    uid = 4044
    # OPEN session (only a buy) — cash spent is ~-650 but realized must be 0.
    _insert_fill(uid, 501, "long", 0.01, 65000.0, fee=0.1)
    m_open = get_session_live_metrics(501, "mainnet", user_id=uid)
    assert abs(m_open["net_base"] - 0.01) < 1e-9
    assert abs(m_open["realized_pnl"]) < 1e-9          # NOT -650

    # FLAT round-trip session — buy 0.01@65000, sell 0.01@65100 -> realized +1.
    _insert_fill(uid, 502, "long", 0.01, 65000.0, fee=0.1)
    _insert_fill(uid, 502, "short", 0.01, 65100.0, fee=0.1)
    m_flat = get_session_live_metrics(502, "mainnet", user_id=uid)
    assert abs(m_flat["net_base"]) < 1e-9
    assert abs(m_flat["realized_pnl"] - 1.0) < 1e-6    # 651 - 650 = +1


def test_session_realized_ignores_void_venue_realized_x18():
    """Regression for the "$24 PnL shown as 0" bug.

    In production ``nado_sync`` enriches every engine fill with
    ``realized_pnl_x18 = 0`` because this venue's indexer match has NO per-fill
    realized PnL field. The OLD code treated a non-null ``realized_pnl_x18`` as
    venue-authoritative and returned that 0, so a profitable flat run reported $0.
    The fix derives realized PnL from signed cash flow regardless — a flat
    round-trip must report +1, and an OPEN residual must report 0 (never the raw
    cash spent)."""
    from src.nadobro.db import execute
    from src.nadobro.models.database import get_session_live_metrics

    execute(_TRADES_DDL)
    execute("DELETE FROM trades_mainnet WHERE user_id = 4046")
    uid = 4046

    def _synced(sid, side, base, price, sub_idx, fee=0.1):
        # Mirrors a venue-synced row: submission_idx set, x18 cash columns set,
        # realized_pnl_x18 = 0 (the venue provides none).
        execute(
            "INSERT INTO trades_mainnet (user_id,product_id,product_name,order_type,side,status,"
            "source,size,price,fill_size,fill_price,fill_fee,submission_idx,realized_pnl_x18,"
            "quote_filled_x18,base_filled_x18,strategy_session_id) "
            "VALUES (%s,2,'BTC-PERP','match',%s,'filled','strategy',%s,%s,%s,%s,%s,%s,0,%s,%s,%s)",
            (uid, side, base, price, base, price, fee, sub_idx,
             int(round(base * price * 1e18)), int(round(base * 1e18)), sid),
        )

    # FLAT round-trip, fully venue-synced (realized_pnl_x18 = 0 on every row):
    # realized must be the signed cash flow +1, NOT the void venue 0.
    _synced(601, "long", 0.01, 65000.0, 1001)
    _synced(601, "short", 0.01, 65100.0, 1002)
    m = get_session_live_metrics(601, "mainnet", user_id=uid)
    assert abs(m["net_base"]) < 1e-9
    assert abs(m["realized_pnl"] - 1.0) < 1e-6

    # OPEN residual, fully venue-synced: realized must be 0 (carried by uPnL),
    # never the raw cash spent (the -$506 / ±notional bug).
    _synced(602, "long", 0.01, 65000.0, 1003)
    m_open = get_session_live_metrics(602, "mainnet", user_id=uid)
    assert abs(m_open["net_base"] - 0.01) < 1e-9
    assert abs(m_open["realized_pnl"]) < 1e-9


def test_session_metrics_ignore_oversized_manual_flatten_row():
    """Regression for the corrupted-attribution bug (session 47/51 style).

    The account-wide stop flatten records ONE synthetic ``source='manual'`` close
    sized to the WHOLE venue position (not the session's own size) and inherits
    the session id from a matched open trade. A grid that opened 0.01 BTC and
    round-tripped flat must NOT be dragged non-flat (and its PnL inflated) by an
    oversized 0.5 BTC manual close attributed to it — that row is excluded from
    session metrics."""
    from src.nadobro.db import execute
    from src.nadobro.models.database import get_session_live_metrics

    execute(_TRADES_DDL)
    execute("DELETE FROM trades_mainnet WHERE user_id = 4048")
    uid = 4048

    # Engine round-trip (source='strategy'): flat, +1 realized.
    _insert_fill(uid, 701, "long", 0.01, 65000.0, fee=0.1)
    _insert_fill(uid, 701, "short", 0.01, 65100.0, fee=0.1)
    # Oversized synthetic flatten close (source='manual', no venue digest/idx),
    # mis-tagged to this session: 0.5 BTC short — 50x the run's own size.
    execute(
        "INSERT INTO trades_mainnet (user_id,product_id,product_name,order_type,side,status,"
        "source,size,price,fill_size,fill_price,fill_fee,strategy_session_id) "
        "VALUES (%s,2,'BTC-PERP','MARKET_CLOSE','short','closed','manual',0.5,65000.0,0.5,65000.0,5.0,701)",
        (uid,),
    )

    m = get_session_live_metrics(701, "mainnet", user_id=uid)
    assert m["fills"] == 2                              # manual row excluded
    assert abs(m["net_base"]) < 1e-9                    # flat — NOT -0.49
    assert abs(m["realized_pnl"] - 1.0) < 1e-6          # +1 — NOT a huge loss


def test_session_live_metrics_strict_per_session_isolation():
    from src.nadobro.db import execute
    from src.nadobro.models.database import get_session_live_metrics

    execute(_TRADES_DDL)
    execute("DELETE FROM trades_mainnet WHERE user_id = 4040")

    uid = 4040
    other = 7777
    execute("DELETE FROM trades_mainnet WHERE user_id = %s", (other,))
    # Session 301: the real dgrid run — one tiny long, 0.0016 @ 65000.
    _insert_fill(uid, 301, "long", 0.0016, 65000.0, fee=0.05)
    # Session 302: a DIFFERENT overlapping run on the same product — big short.
    _insert_fill(uid, 302, "short", 5.0, 65000.0, fee=10.0)
    # Untagged account-only match on the same product (NULL session): must be
    # excluded from BOTH sessions (this is what used to contaminate via window).
    execute(
        "INSERT INTO trades_mainnet (user_id,product_id,product_name,order_type,side,status,source,"
        "size,price,fill_size,fill_price,fill_fee,strategy_session_id) "
        "VALUES (%s,2,'BTC-PERP','match','long','filled','strategy',3.0,64000.0,3.0,64000.0,7.0,NULL)",
        (uid,),
    )
    # CROSS-USER contamination row: ANOTHER user's fill mistakenly carrying the
    # SAME strategy_session_id (301). The user pin must exclude it — no user's
    # PnL may ever leak into another's (the -$302-across-platform concern).
    _insert_fill(other, 301, "short", 100.0, 65000.0, fee=999.0)

    m301 = get_session_live_metrics(301, "mainnet", user_id=uid)
    m302 = get_session_live_metrics(302, "mainnet", user_id=uid)

    # Uniqueness + Isolation: each session sees ONLY this user's own fill.
    assert m301["fills"] == 1
    assert m302["fills"] == 1
    # net_base: long +0.0016 for 301 (NOT polluted by the other user's -100 short);
    # short -5.0 for 302.
    assert abs(m301["net_base"] - 0.0016) < 1e-9
    assert abs(m302["net_base"] - (-5.0)) < 1e-9
    # signed_cash: long -> -quote; short -> +quote.
    assert abs(m301["signed_cash"] - (-(0.0016 * 65000.0))) < 1e-6
    assert abs(m302["signed_cash"] - (5.0 * 65000.0)) < 1e-6
    # fees are per-user, per-session — not summed across runs, the untagged row,
    # or the other user's 999.0 fee.
    assert abs(m301["fees"] - 0.05) < 1e-9
    assert abs(m302["fees"] - 10.0) < 1e-9

    # And the OTHER user's session-301 view sees only THEIR fill, never uid's.
    m_other = get_session_live_metrics(301, "mainnet", user_id=other)
    assert m_other["fills"] == 1
    assert abs(m_other["net_base"] - (-100.0)) < 1e-9


def test_snapshot_uses_venue_upnl_and_real_turnover():
    """End-to-end against real Postgres: session unrealized == the VENUE position
    uPnL (so the SL agrees with Portfolio), volume == real turnover on the product
    (not the under-counted tagged-fill sum), realized == venue per-match PnL."""
    from unittest.mock import patch
    from datetime import datetime, timezone
    from src.nadobro.db import execute
    from src.nadobro.services import live_session

    execute(_TRADES_DDL)
    execute("DELETE FROM trades_mainnet WHERE user_id = 4041")
    uid = 4041
    started = datetime(2020, 1, 1, tzinfo=timezone.utc)
    # Real turnover: several fills on the product in the run window (some tagged
    # to the session, some not) — turnover counts ALL of them for user+product.
    _insert_fill(uid, 401, "long", 0.03, 65000.0, fee=0.5)
    _insert_fill(uid, 401, "long", 0.03, 64000.0, fee=0.5)
    # An untagged fill on the same product (e.g. position close via archive path)
    # — excluded from the tagged metric but INCLUDED in turnover (matches Nado).
    execute(
        "INSERT INTO trades_mainnet (user_id,product_id,product_name,order_type,side,status,source,"
        "size,price,fill_size,fill_price,fill_fee,strategy_session_id) "
        "VALUES (%s,2,'BTC-PERP','match','long','filled','strategy',0.02,63000.0,0.02,63000.0,0.3,NULL)",
        (uid,),
    )

    sess = {"id": 401, "product_id": 2, "started_at": started, "stopped_at": None}
    venue = {"size_signed": 0.08, "entry": 64000.0, "liq": 60000.0, "leverage": 49.0,
             "margin_used": 100.0, "upnl": -10.38, "synced_ts": 9e18}
    with patch.object(live_session, "_venue_position", return_value=venue):
        snap = live_session.get_live_session_snapshot(
            uid, "mainnet", sess, state={"notional_usd": 100.0}, client=None, mark=63135.0,
        )
    # Unrealized is the venue uPnL (baseline 0) -> SL of 10% would fire here.
    assert abs(snap["unrealized_pnl"] - (-10.38)) < 1e-9
    assert abs(snap["session_pnl_pct"] - (-10.38)) < 0.5
    assert abs(snap["position_size"] - 0.08) < 1e-9
    # Turnover spans all three fills on the product: 0.03*65000 + 0.03*64000 + 0.02*63000
    expected_turnover = 0.03 * 65000.0 + 0.03 * 64000.0 + 0.02 * 63000.0
    assert abs(snap["volume"] - expected_turnover) < 1e-3
