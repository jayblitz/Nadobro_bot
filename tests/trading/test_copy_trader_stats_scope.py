"""Per-user scoping of copy trader stats.

Curated traders are visible to every user, but the hub/preview stats must be
the VIEWER's own results with that trader — never a pool of all followers'
PnL/volume (that both misleads and leaks other users' outcomes). The global
aggregate (user_id=None) stays available for admin/ops callers.
"""

import pytest

import src.nadobro.db as db_mod
import src.nadobro.trading.copy_service as copy_service

_MIRRORS = [
    {"id": 1, "user_id": 1001, "cumulative_pnl": 100.0, "cumulative_fees_usd": 10.0,
     "cumulative_volume_usd": 5000.0},
    {"id": 2, "user_id": 2002, "cumulative_pnl": -40.0, "cumulative_fees_usd": 4.0,
     "cumulative_volume_usd": 2000.0},
]


@pytest.fixture()
def _stub_data(monkeypatch):
    monkeypatch.setattr(copy_service, "get_mirrors_for_trader", lambda tid: list(_MIRRORS))
    # Closed positions per mirror: mirror 1 -> one win, mirror 2 -> one loss.
    monkeypatch.setattr(
        db_mod, "query_all",
        lambda sql, params=None: [{"pnl": 25.0}] if params == (1,) else [{"pnl": -12.0}],
    )


def test_stats_scoped_to_requesting_user(_stub_data):
    stats = copy_service.get_trader_stats(7, user_id=1001)
    assert stats["pnl_usd"] == pytest.approx(90.0)      # 100 - 10, user 1001 only
    assert stats["volume_usd"] == pytest.approx(5000.0)
    assert stats["total_trades"] == 1
    assert stats["win_rate"] == pytest.approx(100.0)


def test_other_user_sees_their_own_numbers(_stub_data):
    stats = copy_service.get_trader_stats(7, user_id=2002)
    assert stats["pnl_usd"] == pytest.approx(-44.0)     # -40 - 4
    assert stats["volume_usd"] == pytest.approx(2000.0)
    assert stats["win_rate"] == pytest.approx(0.0)


def test_unscoped_call_keeps_global_aggregate(_stub_data):
    stats = copy_service.get_trader_stats(7)
    assert stats["pnl_usd"] == pytest.approx(46.0)      # (100-10) + (-40-4)
    assert stats["volume_usd"] == pytest.approx(7000.0)
    assert stats["total_trades"] == 2


def test_user_with_no_mirrors_gets_zeroes(_stub_data):
    stats = copy_service.get_trader_stats(7, user_id=3003)
    assert stats["pnl_usd"] == 0.0
    assert stats["volume_usd"] == 0.0
    assert stats["total_trades"] == 0
    assert stats["win_rate"] == 0.0
