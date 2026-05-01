from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_text_trade_replies_use_supported_parse_mode_constant():
    source = (ROOT / "src/nadobro/handlers/intent_handlers.py").read_text()

    assert 'parse_mode="MARKDOWN_V2"' not in source
    assert "parse_mode=ParseMode.MARKDOWN_V2" in source


def test_init_db_contains_portfolio_sync_columns():
    source = (ROOT / "src/nadobro/db.py").read_text()

    for required in (
        "ALTER TABLE positions ADD COLUMN IF NOT EXISTS synced_at",
        "ALTER TABLE positions ADD COLUMN IF NOT EXISTS product_id",
        "ALTER TABLE positions ADD COLUMN IF NOT EXISTS isolated",
        "ALTER TABLE open_orders ADD COLUMN IF NOT EXISTS synced_at",
        "ALTER TABLE open_orders ADD COLUMN IF NOT EXISTS product_id",
        "CREATE UNIQUE INDEX IF NOT EXISTS open_orders_unique_digest",
        "CREATE TABLE IF NOT EXISTS sync_log",
        "CREATE TABLE IF NOT EXISTS funding_payments_mainnet",
        "CREATE TABLE IF NOT EXISTS funding_payments_testnet",
    ):
        assert required in source


def test_portfolio_deck_does_not_return_unbounded_cached_snapshot():
    source = (ROOT / "src/nadobro/handlers/portfolio_deck.py").read_text()

    assert "get_cached_snapshot" not in source
    assert "return await sync_user" in source


def test_trade_card_has_markdown_fallback_for_edits():
    source = (ROOT / "src/nadobro/handlers/trade_card.py").read_text()

    assert "Can't parse entities" in source
    assert "plain_text_fallback" in source
