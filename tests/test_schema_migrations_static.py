from pathlib import Path


def test_concurrency_schema_migration_covers_startup_ddl_additions():
    sql = Path("src/nadobro/migrations/0005_concurrency_and_copy_constraints.sql").read_text()

    assert "ALTER TABLE fill_sync_queue" in sql
    assert "claimed_at" in sql
    assert "CREATE TABLE IF NOT EXISTS order_intents" in sql
    assert "order_intents_status_check" in sql
    assert "ALTER TABLE copy_positions" in sql
    assert "tp_order_digest" in sql
    assert "sl_order_digest" in sql


def test_migration_sequence_has_no_gap():
    migration_dir = Path("src/nadobro/migrations")
    numbers = sorted(int(path.name.split("_", 1)[0]) for path in migration_dir.glob("*.sql"))

    assert numbers == list(range(1, max(numbers) + 1))


def test_desk_plans_migration_covers_startup_ddl():
    sql = Path("src/nadobro/migrations/0012_desk_plans.sql").read_text()
    for net in ("testnet", "mainnet"):
        assert f"CREATE TABLE IF NOT EXISTS desk_plans_{net}" in sql
        assert f"idx_desk_plans_{net}_user_status" in sql
        assert f"idx_desk_plans_{net}_active" in sql
    # the guarded-transition contract relies on these statuses exactly
    for status in ("draft", "awaiting_trigger", "running",
                   "completed", "cancelled", "failed"):
        assert status in sql


def test_engine_v2_migration_covers_new_tables():
    sql = Path("src/nadobro/migrations/0007_engine_v2_tables.sql").read_text()
    assert "CREATE TABLE IF NOT EXISTS engine_executors" in sql
    assert "CREATE TABLE IF NOT EXISTS engine_position_hold" in sql
    assert "CREATE TABLE IF NOT EXISTS engine_portfolio_history" in sql
    assert "CREATE TABLE IF NOT EXISTS engine_strategy_sessions" in sql
    assert "ix_engine_executors_user_ctrl" in sql
