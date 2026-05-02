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
