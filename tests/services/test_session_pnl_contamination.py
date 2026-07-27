"""SESSION-PNL-CONTAMINATION: a session's realized PnL must come from its OWN fills.

Reported 2026-07-27: /mm_status showed ``realized $+205.72 (+82.55% of $250
margin)`` and the take-profit rail stopped the bot — while the run's real PnL was
about **$1**. The numbers decode exactly:

    (65,230.94 - 58,912.60) x 0.03256 BTC = $205.72

The session's own closed size was 0.01910 BTC (0.00315 + 0.00955 + 0.00640) and
its fills traded in 65,145-65,259. The replay had closed **1.7x** that size
against a blended entry from long before the run started — i.e. fills that
PREDATE the session were tagged into it, so the entry basis came from another
era's inventory.

Fix: the replay window is pinned to ``strategy_sessions.started_at`` (a session's
fills can never predate the run), plus diagnostics that dump the offending rows
when realized PnL is not achievable from the rows' own price range.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from src.nadobro.models import database as db
from src.nadobro.quant.portfolio_calculator import realized_pnl_windows_from_rows

T0 = datetime(2026, 7, 27, 8, 34, tzinfo=timezone.utc)


def _fill(side, size, price, mins, isolated=False, source="strategy"):
    return {
        "product_id": 2, "side": side, "isolated": isolated, "source": source,
        "fill_size": size, "size": size, "fill_price": price, "price": price,
        "base_filled_x18": str(int(size * 1e18)),
        "quote_filled_x18": str(int(size * price * 1e18)),
        "submission_idx": f"{mins}{size}",
        "filled_at": T0 + timedelta(minutes=mins),
    }


# The run's REAL fills, exactly as they appear in the Nado trade history.
_SESSION_FILLS = [
    _fill("long", 0.00955, 65145, 0),
    _fill("long", 0.00955, 65145, 0),
    _fill("short", 0.00315, 65202, 51),
    _fill("short", 0.00955, 65213, 59),
    _fill("short", 0.00640, 65259, 61),
]


def test_replay_of_the_sessions_own_fills_matches_nado():
    """Sanity anchor: Nado shows +0.73 +0.65 +0.18 = $1.56."""
    out = realized_pnl_windows_from_rows(_SESSION_FILLS)
    assert abs(float(out["total_pnl"]) - 1.5586) < 0.01


def test_fills_predating_the_session_are_excluded_by_the_query():
    """The guard lives in SQL: the replay window starts at started_at."""
    import inspect

    sql = inspect.getsource(db._derive_session_realized_pnl)
    assert "started_at" in sql, "session replay must be bounded by started_at"
    assert ">= COALESCE(" in sql
    # session_id is bound twice (product fallback + started_at), around `where`.
    captured = {}

    def _fake_query_all(query, params=None):
        captured["query"] = query
        captured["params"] = params
        return []

    with patch.object(db, "query_all", _fake_query_all):
        db._derive_session_realized_pnl(
            "trades_mainnet", "strategy_session_id = %s AND user_id = %s", [7, 42], 7
        )
    # 2 literal placeholders + 2 from the injected WHERE == 4 args.
    assert captured["query"].count("%s") == len(captured["params"]) == 4
    assert captured["params"][0] == 7 and captured["params"][-1] == 7


def test_a_pre_session_fill_would_have_inflated_realized_pnl():
    """Reproduces the contamination the guard now prevents: one cheap fill from
    before the run drags the entry basis down and explodes realized PnL."""
    stale = _fill("long", 0.0191, 58912.60, -600)     # 10h before the run
    clean = float(realized_pnl_windows_from_rows(_SESSION_FILLS)["total_pnl"])
    dirty = float(realized_pnl_windows_from_rows([stale] + _SESSION_FILLS)["total_pnl"])
    assert clean < 2.0, "the run's own fills are worth ~$1.56"
    # One stale fill is enough to explode it by more than an order of magnitude
    # — the same mechanism that turned ~$1 into $205.72 in production.
    assert dirty > 20 * clean, "the pre-session fill must inflate realized PnL"
    assert dirty > 50.0


# ── diagnostics ─────────────────────────────────────────────────

def test_diagnostics_dump_rows_when_realized_is_impossible(caplog):
    """|realized| can never exceed price_range * base_traded for the SAME rows.
    Exceeding it proves foreign rows are in the replay — dump them."""
    with caplog.at_level(logging.ERROR):
        db._log_session_replay_diagnostics(99, _SESSION_FILLS, realized=205.72)
    text = caplog.text
    assert "SESSION-PNL-CONTAMINATION" in text
    assert "session=99" in text
    assert "realized=205.7200" in text
    # every consumed row is dumped so the foreign one is identifiable
    assert text.count("fill side=") == len(_SESSION_FILLS)


def test_diagnostics_stay_silent_for_a_plausible_result(caplog):
    with caplog.at_level(logging.ERROR):
        db._log_session_replay_diagnostics(99, _SESSION_FILLS, realized=1.5586)
    assert "SESSION-PNL-CONTAMINATION" not in caplog.text


def test_diagnostics_never_raise_on_bad_rows(caplog):
    for rows in ([], [{}], [{"fill_price": "x", "fill_size": None}]):
        db._log_session_replay_diagnostics(1, rows, realized=1.0)   # must not raise
