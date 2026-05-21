"""Static checks for the Engine v2 Phase 2 migrations (0007 create / 0008 drop)."""
from __future__ import annotations

from pathlib import Path

MIGRATIONS = Path("src/nadobro/migrations")


def _read(name: str) -> str:
    return (MIGRATIONS / name).read_text()


def test_0007_creates_four_engine_tables_with_indexes():
    sql = _read("0007_engine_v2_tables.sql")
    for table in (
        "engine_executors",
        "engine_position_hold",
        "engine_portfolio_history",
        "engine_strategy_sessions",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in sql
    for index in (
        "ix_engine_executors_user_ctrl",
        "ix_engine_executors_pair",
        "ix_engine_executors_state",
    ):
        assert index in sql
    assert "PRIMARY KEY (user_id, trading_pair, controller_id)" in sql
    assert "PRIMARY KEY (user_id, ts)" in sql
    assert "UNIQUE (user_id, controller_id, session_n)" in sql


def test_0007_is_fk_free_by_design():
    assert "REFERENCES" not in _read("0007_engine_v2_tables.sql")


def test_0008_drops_only_orphan_legacy_tables():
    sql = _read("0008_drop_legacy_orphan_tables.sql")
    assert "DROP TABLE IF EXISTS strategies" in sql
    assert "DROP TABLE IF EXISTS strategy_performance_snapshots" in sql
    # strategy_sessions is still used by kept features and must NOT be dropped.
    assert "DROP TABLE IF EXISTS strategy_sessions" not in sql


def test_engine_tables_also_created_idempotently_in_db_init():
    db = Path("src/nadobro/db.py").read_text()
    for table in (
        "engine_executors",
        "engine_position_hold",
        "engine_portfolio_history",
        "engine_strategy_sessions",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in db
    # legacy CREATE statements are gone from db.py init
    assert "CREATE TABLE IF NOT EXISTS strategies (" not in db
    assert "CREATE TABLE IF NOT EXISTS strategy_performance_snapshots (" not in db
