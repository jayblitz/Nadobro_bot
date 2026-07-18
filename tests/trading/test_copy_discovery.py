"""Copy-discovery ranking: active repeatable ROI beats stale absolute PnL."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.nadobro.market_data import nadoexplorer_client as explorer
from src.nadobro.trading import copy_discovery


def _row(wallet: str, *, roi: float, active_days: int, age_hours: float,
         closed_trades: int, drawdown: float, pnl: float) -> dict:
    return {
        "wallet_address": wallet,
        "roi": roi,
        "active_days": active_days,
        "period_days": 30,
        "last_activity_at": (datetime.now(timezone.utc) - timedelta(hours=age_hours)).isoformat(),
        "closed_trades": closed_trades,
        "max_drawdown_pct": drawdown,
        "win_rate": 0.60,
        "pnl_usd": pnl,
    }


def test_quality_rank_rejects_stale_high_pnl_and_prefers_active_roi():
    rows = [
        _row(
            "0xrecentgood",
            roi=0.55,
            active_days=26,
            age_hours=1,
            closed_trades=600,
            drawdown=0.10,
            pnl=15_000,
        ),
        _row(
            "0xstalehuge",
            roi=3.00,
            active_days=29,
            age_hours=24 * 20,
            closed_trades=40_000,
            drawdown=0.05,
            pnl=9_000_000,
        ),
        _row(
            "0xactivepnl",
            roi=0.08,
            active_days=25,
            age_hours=1,
            closed_trades=600,
            drawdown=0.10,
            pnl=500_000,
        ),
    ]

    ranked = copy_discovery._rank_quality_rows(rows)

    assert [row["wallet_address"] for row in ranked] == ["0xrecentgood", "0xactivepnl"]


def test_quality_rank_rejects_unknown_win_rate_or_drawdown():
    valid = _row(
        "0xvalid",
        roi=0.40,
        active_days=25,
        age_hours=1,
        closed_trades=200,
        drawdown=0.10,
        pnl=10_000,
    )
    missing_win_rate = {**valid, "wallet_address": "0xmissing", "win_rate": None}
    nonnumeric_drawdown = {
        **valid,
        "wallet_address": "0xnonnumeric",
        "max_drawdown_pct": "unknown",
    }

    ranked = copy_discovery._rank_quality_rows(
        [missing_win_rate, nonnumeric_drawdown, valid]
    )

    assert [row["wallet_address"] for row in ranked] == ["0xvalid"]


def test_quality_page_paginates_complete_roi_pool_before_slicing(monkeypatch):
    first_page = [
        _row(
            f"0x{i:040x}",
            roi=0.05,
            active_days=20,
            age_hours=1,
            closed_trades=100,
            drawdown=0.20,
            pnl=1_000 + i,
        )
        for i in range(copy_discovery.QUALITY_CANDIDATE_PAGE_SIZE)
    ]
    late_best = _row(
        "0xffffffffffffffffffffffffffffffffffffffff",
        roi=2.00,
        active_days=30,
        age_hours=0.5,
        closed_trades=1_000,
        drawdown=0.01,
        pnl=2_000,
    )
    offsets = []

    def _result(**kwargs):
        offsets.append(kwargs["offset"])
        assert kwargs["sort"] == "roi"
        assert kwargs["limit"] == copy_discovery.QUALITY_CANDIDATE_PAGE_SIZE
        assert kwargs["min_active_days"] == copy_discovery.MIN_LEADER_ACTIVE_DAYS
        if kwargs["offset"] == 0:
            return {"rows": first_page, "has_more": True}
        if kwargs["offset"] == copy_discovery.QUALITY_CANDIDATE_PAGE_SIZE:
            return {"rows": [late_best], "has_more": False}
        raise AssertionError(f"unexpected offset {kwargs['offset']}")

    monkeypatch.setattr(explorer, "get_leaderboard_result", _result)

    result = copy_discovery.leaderboard_page(0)

    assert offsets == [0, copy_discovery.QUALITY_CANDIDATE_PAGE_SIZE]
    assert result.available and result.has_more and len(result.rows) == 5
    assert result.rows[0]["wallet_address"] == late_best["wallet_address"]
    assert result.rows[0]["rank"] == 1


def test_quality_page_scores_before_slicing_and_never_marks_empty_as_outage(monkeypatch):
    rows = [
        _row(
            f"0x{i:040d}",
            roi=0.20 + i / 100,
            active_days=20,
            age_hours=1,
            closed_trades=100,
            drawdown=0.10,
            pnl=1000 * i,
        )
        for i in range(6)
    ]
    captured = {}

    def _result(**kwargs):
        captured.update(kwargs)
        return {"rows": rows, "has_more": False}

    monkeypatch.setattr(explorer, "get_leaderboard_result", _result)

    first = copy_discovery.leaderboard_page(0)
    second = copy_discovery.leaderboard_page(1)

    assert captured["sort"] == "roi"
    assert captured["min_active_days"] == copy_discovery.MIN_LEADER_ACTIVE_DAYS
    assert first.available and first.has_more and len(first.rows) == 5
    assert second.available and not second.has_more and len(second.rows) == 1
    assert first.rows[0]["roi"] > first.rows[-1]["roi"]

    monkeypatch.setattr(explorer, "get_leaderboard_result", lambda **_: {"rows": [], "has_more": False})
    empty = copy_discovery.leaderboard_page(0)
    assert empty.available
    assert empty.rows == []


def test_quality_page_fails_closed_when_a_later_page_is_unavailable(monkeypatch):
    first_page = [
        _row(
            f"0x{i:040x}",
            roi=0.20,
            active_days=20,
            age_hours=1,
            closed_trades=100,
            drawdown=0.10,
            pnl=1_000,
        )
        for i in range(copy_discovery.QUALITY_CANDIDATE_PAGE_SIZE)
    ]
    offsets = []

    def _result(**kwargs):
        offsets.append(kwargs["offset"])
        if kwargs["offset"] == 0:
            return {"rows": first_page, "has_more": True}
        return None

    monkeypatch.setattr(explorer, "get_leaderboard_result", _result)

    result = copy_discovery.leaderboard_page(0)

    assert offsets == [0, copy_discovery.QUALITY_CANDIDATE_PAGE_SIZE]
    assert not result.available
    assert not result.has_more
    assert result.rows == []


def test_quality_page_fails_closed_at_the_safe_pagination_cap(monkeypatch):
    row = _row(
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        roi=0.20,
        active_days=20,
        age_hours=1,
        closed_trades=100,
        drawdown=0.10,
        pnl=1_000,
    )
    calls = []

    def _result(**kwargs):
        calls.append(kwargs["offset"])
        return {"rows": [{**row, "wallet_address": f"0x{kwargs['offset']:040x}"}], "has_more": True}

    monkeypatch.setattr(explorer, "get_leaderboard_result", _result)
    monkeypatch.setattr(copy_discovery, "MAX_QUALITY_CANDIDATE_PAGES", 2)

    result = copy_discovery.leaderboard_page(0)

    assert calls == [0, copy_discovery.QUALITY_CANDIDATE_PAGE_SIZE]
    assert not result.available
    assert result.rows == []
