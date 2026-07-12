import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.models import database


async def _inline_run_blocking(func, *args, **kwargs):
    return func(*args, **kwargs)


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


def test_sync_pending_fills_does_not_cancel_recorded_fill_when_archive_missing():
    from src.nadobro.venue import nado_archive
    from src.nadobro.runtime import scheduler
    from src.nadobro.users import user_service

    entry = {
        "id": 7,
        "trade_id": 42,
        "network": "mainnet",
        "order_digest": "0xfilled",
        "attempts": 20,
        "user_id": 1001,
        "product_id": 2,
        "subaccount_hex": "0xsub",
    }
    trade_row = {"id": 42, "status": "filled", "size": 1, "fill_size": 0}
    releases = []
    updates = []

    with (
        patch.object(database, "claim_pending_fill_syncs", return_value=[entry]),
        patch.object(database, "get_trade_by_id", return_value=trade_row),
        patch.object(database, "release_fill_sync", side_effect=lambda sync_id: releases.append(sync_id)),
        patch.object(database, "resolve_fill_sync") as resolve_fill_sync,
        patch.object(database, "expire_fill_sync"),
        patch.object(database, "update_trade", side_effect=lambda *args, **kwargs: updates.append((args, kwargs))),
        patch.object(database, "increment_session_metrics"),
        patch.object(nado_archive, "is_archive_rate_limited", return_value=False),
        patch.object(nado_archive, "query_orders_by_digests", return_value={}),
        patch.object(nado_archive, "query_order_by_digest", return_value=None),
        patch.object(nado_archive, "query_orders_by_subaccount", return_value=[]),
        patch.object(user_service, "get_user_nado_client", return_value=MagicMock()),
        patch.object(scheduler, "run_blocking", new=_inline_run_blocking),
        patch.object(scheduler, "_notify_limit_order_cancelled_once", new=AsyncMock()),
    ):
        asyncio.run(scheduler.sync_pending_fills())

    assert releases == [7]
    assert updates == []
    resolve_fill_sync.assert_not_called()


def test_sync_pending_fills_does_not_cancel_after_digest_poll_hits_rate_limit():
    from src.nadobro.venue import nado_archive
    from src.nadobro.runtime import scheduler
    from src.nadobro.users import user_service

    entry = {
        "id": 8,
        "trade_id": 43,
        "network": "mainnet",
        "order_digest": "0xpending",
        "attempts": 20,
        "user_id": 1001,
        "product_id": 2,
        "subaccount_hex": "0xsub",
    }
    trade_row = {"id": 43, "status": "pending", "size": 1}
    releases = []
    client = MagicMock()

    with (
        patch.object(database, "claim_pending_fill_syncs", return_value=[entry]),
        patch.object(database, "get_trade_by_id", return_value=trade_row),
        patch.object(database, "release_fill_sync", side_effect=lambda sync_id: releases.append(sync_id)),
        patch.object(database, "resolve_fill_sync") as resolve_fill_sync,
        patch.object(database, "expire_fill_sync"),
        patch.object(database, "update_trade") as update_trade,
        patch.object(database, "increment_session_metrics"),
        patch.object(nado_archive, "is_archive_rate_limited", side_effect=[False, False, False, True]),
        patch.object(nado_archive, "query_orders_by_digests", return_value={}),
        patch.object(nado_archive, "query_order_by_digest", return_value=None),
        patch.object(nado_archive, "query_orders_by_subaccount", return_value=[]),
        patch.object(user_service, "get_user_nado_client", return_value=client),
        patch.object(scheduler, "run_blocking", new=_inline_run_blocking),
        patch.object(scheduler, "_notify_limit_order_cancelled_once", new=AsyncMock()),
    ):
        asyncio.run(scheduler.sync_pending_fills())

    assert releases == [8]
    update_trade.assert_not_called()
    resolve_fill_sync.assert_not_called()
    client.get_open_orders.assert_not_called()


def test_market_snapshot_helpers_are_defined():
    """AUDIT-FIX-SCH-2 regression: a main-branch merge deleted the
    ``_market_snapshot_lock_get`` helper while leaving the call site in
    ``_get_market_snapshot``. The result was a ``NameError`` raised on every
    alert-tick and price-tracker tick (every 5s in production)."""
    from src.nadobro.runtime import scheduler

    assert callable(getattr(scheduler, "_market_snapshot_lock_get", None))
    assert hasattr(scheduler, "_get_market_snapshot")

    lock = scheduler._market_snapshot_lock_get()
    assert isinstance(lock, asyncio.Lock)
    assert scheduler._market_snapshot_lock_get() is lock


def test_get_market_snapshot_returns_cached_prices_without_nameerror():
    """The actual call site that was raising in production. Force a cached
    hit so we don't need a real check client."""
    from src.nadobro.runtime import scheduler

    cached = {"BTC-PERP": 100.0}
    with (
        patch.object(scheduler, "_check_client", MagicMock()),
        patch.object(
            scheduler,
            "_last_market_snapshot",
            {"ts": float("inf"), "prices": cached},
        ),
    ):
        result = asyncio.run(scheduler._get_market_snapshot())

    assert result == cached
