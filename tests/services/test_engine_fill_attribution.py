"""Engine fill → session attribution (the session-volume undercount fix).

Root cause chain that these guard:
  * Engine order_status fill detection is unreliable, but nado_sync reliably
    syncs every venue fill. If the digest→session intent is only written at
    (flaky) fill-detection time, undetected fills orphan as source='manual',
    session=null — and rollup_session_from_trades counts only source='strategy',
    so session volume is undercounted (e.g. 1 of 6 fills → 6x low).
  * Fix: link digest→session at PLACEMENT (source='strategy'), and make the
    executor recorder idempotent so it never duplicates a fill the venue sync
    already recorded (venue sync can beat the executor by tens of seconds).
"""
from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch

from src.nadobro.services.engine_persistence import DbTradeRecorder
from src.nadobro.engine.types import TradeType


def test_link_placement_writes_intent_at_placement():
    rec = DbTradeRecorder()
    execs: list = []
    with patch(
        "src.nadobro.services.engine_persistence.resolve_running_session_id",
        return_value=90,
    ), patch("src.nadobro.db.execute", side_effect=lambda *a, **k: execs.append(a)):
        rec.link_placement("rgrid:42:mainnet", "0xdeadbeef")

    assert execs, "placement must write an order_intents row"
    sql, params = execs[0][0], execs[0][1]
    assert "order_intents" in sql and "0xdeadbeef" in params
    # The intent value must carry the session AND source='strategy' so
    # nado_sync attributes the venue fill to the session's strategy rollup.
    joined = " ".join(str(p) for p in params)
    assert "90" in joined and "strategy" in joined


def test_link_placement_noop_without_session():
    rec = DbTradeRecorder()
    execs: list = []
    with patch(
        "src.nadobro.services.engine_persistence.resolve_running_session_id",
        return_value=None,
    ), patch("src.nadobro.db.execute", side_effect=lambda *a, **k: execs.append(a)):
        rec.link_placement("rgrid:42:mainnet", "0xabc")
    assert execs == []  # no session → nothing to attribute, don't orphan an intent


def _record(rec, venue_row_exists: bool):
    inserted: list = []
    with patch(
        "src.nadobro.services.engine_persistence.resolve_running_session_id",
        return_value=90,
    ), patch(
        "src.nadobro.services.product_catalog.get_product_id", return_value=2
    ), patch(
        "src.nadobro.db.query_one",
        return_value=({"": 1} if venue_row_exists else None),
    ), patch(
        "src.nadobro.models.database.insert_trade",
        side_effect=lambda data, network="mainnet": inserted.append(data),
    ), patch("src.nadobro.db.execute"):
        rec.record(
            "rgrid:42:mainnet", "BTC", TradeType.BUY,
            Decimal("0.0056"), Decimal("59353.68"), Decimal("0.03"),
            order_id="0xd5eb271ac172",
        )
    return inserted


def test_recorder_skips_when_venue_already_recorded_fill():
    # Venue sync already wrote this digest (has a submission_idx) → the executor
    # recorder must NOT insert a duplicate strategy row (would double-count).
    assert _record(DbTradeRecorder(), venue_row_exists=True) == []


def test_recorder_inserts_when_no_venue_row_yet():
    # No venue row for the digest → the recorder inserts (venue sync will later
    # ENRICH this row, not duplicate it).
    inserted = _record(DbTradeRecorder(), venue_row_exists=False)
    assert len(inserted) == 1
    assert inserted[0]["source"] == "strategy"
    assert inserted[0]["strategy_session_id"] == 90


def test_recorder_resolves_dn_spot_leg_product_id():
    rec = DbTradeRecorder()
    inserted: list = []

    with patch(
        "src.nadobro.services.engine_persistence.resolve_running_session_id",
        return_value=90,
    ), patch(
        "src.nadobro.services.product_catalog.get_spot_product_id", return_value=118
    ), patch(
        "src.nadobro.services.product_catalog.get_product_id",
        side_effect=AssertionError("DN spot leg must not use perp resolver"),
    ), patch(
        "src.nadobro.config.get_product_name", return_value="WGOOGLX"
    ), patch(
        "src.nadobro.models.database.insert_trade",
        side_effect=lambda data, network="mainnet": inserted.append(data),
    ):
        rec.record(
            "dn:42:mainnet", "WGOOGLX-USDT0", TradeType.BUY,
            Decimal("0.5"), Decimal("700"), Decimal("0.07"),
        )

    assert len(inserted) == 1
    assert inserted[0]["product_id"] == 118
    assert inserted[0]["product_name"] == "WGOOGLX"
    assert inserted[0]["strategy_session_id"] == 90


def test_recorder_records_sessionless_desk_fill_as_manual():
    """Desk plans drive the engine WITHOUT a strategy session. Their fills are
    user-initiated trades: record them as MANUAL (digest + product carried) so
    the venue match enriches the row and History round-trips pair it — the old
    skip left desk fills to sync in as unattributable product_id=0 rows
    (prod 2026-07-09: text-to-trade desk orders invisible in History)."""
    rec = DbTradeRecorder()
    inserted: list = []
    tagged: list = []
    stats: list = []
    with patch(
        "src.nadobro.services.engine_persistence.resolve_running_session_id",
        return_value=None,
    ), patch(
        "src.nadobro.services.engine_persistence._resolve_engine_fill_product",
        return_value=(2, "BTC-PERP"),
    ), patch(
        "src.nadobro.models.database.insert_trade",
        side_effect=lambda d, network=None: inserted.append(d) or 1,
    ), patch(
        "src.nadobro.services.order_intents.link_digest_intent",
        side_effect=lambda *a, **k: tagged.append(k) or True,
    ), patch("src.nadobro.db.query_one", return_value=None), patch(
        "src.nadobro.services.user_service.update_trade_stats",
        side_effect=lambda uid, vol, **k: stats.append((uid, vol)),
    ):
        rec.record(
            "desk:42:mainnet", "BTC-PERP", TradeType.SELL,
            "0.035", "63047.87775", "0.94927875", order_id="0xdesk",
        )

    assert len(inserted) == 1
    data = inserted[0]
    assert data["source"] == "manual"
    assert "strategy_session_id" not in data
    assert data["order_digest"] == "0xdesk"
    assert data["side"] == "short"
    assert data["product_id"] == 2
    # Digest also tagged manual (+product) so the venue match stays
    # attributable even if the recorder row is ever lost.
    assert tagged and tagged[0].get("source") == "manual"
    assert tagged[0].get("product_id") == 2
    # Desk volume feeds the user volume counters -> referral stats
    # (update_trade_stats calls record_referred_volume). Platform-placed
    # trades never reach the recorder, so they stay excluded by design.
    assert stats and stats[0][0] == 42
    assert abs(stats[0][1] - 0.035 * 63047.87775) < 0.01


def test_recorder_still_skips_sessionless_non_desk_fill():
    """The stale-controller guard stays: a non-desk engine fill with no
    running session must not orphan a row (misattribution protection)."""
    rec = DbTradeRecorder()
    inserted: list = []
    with patch(
        "src.nadobro.services.engine_persistence.resolve_running_session_id",
        return_value=None,
    ), patch(
        "src.nadobro.models.database.insert_trade",
        side_effect=lambda d, network=None: inserted.append(d) or 1,
    ):
        rec.record(
            "rgrid:42:mainnet", "BTC-PERP", TradeType.BUY,
            "0.01", "63000", "0.4", order_id="0xstale",
        )
    assert inserted == []
