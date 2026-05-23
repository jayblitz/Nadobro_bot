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

    for mig in ("0007_engine_v2_tables.sql", "0009_engine_kill_switch.sql"):
        execute((MIGRATIONS / mig).read_text())
    # clean slate for deterministic assertions
    execute("TRUNCATE engine_position_hold, engine_executors, engine_kill_switch")
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
