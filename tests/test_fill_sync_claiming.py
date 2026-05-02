from unittest.mock import patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.models import database


def test_claim_pending_fill_syncs_claims_rows_atomically():
    captured = {}

    def _query_all(sql, params):
        captured["sql"] = sql
        captured["params"] = params
        return [{"id": 1, "status": "processing", "attempts": 3}]

    with patch.object(database, "query_all", side_effect=_query_all):
        rows = database.claim_pending_fill_syncs(limit=25, stale_after_minutes=7)

    assert rows == [{"id": 1, "status": "processing", "attempts": 3}]
    assert "FOR UPDATE SKIP LOCKED" in captured["sql"]
    assert "status = 'processing'" in captured["sql"]
    assert "attempts = attempts + 1" in captured["sql"]
    assert captured["params"] == (7, 25)


def test_release_fill_sync_only_releases_claimed_rows():
    captured = {}

    def _execute(sql, params):
        captured["sql"] = sql
        captured["params"] = params

    with patch.object(database, "execute", side_effect=_execute):
        database.release_fill_sync(42)

    assert "status = 'pending'" in captured["sql"]
    assert "status = 'processing'" in captured["sql"]
    assert captured["params"] == (42,)
