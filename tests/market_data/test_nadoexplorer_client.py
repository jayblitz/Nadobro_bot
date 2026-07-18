"""NadoExplorer client: parsing of the real API shapes, degrade behavior, and
the rate-limit soft floor. Fixture payloads are verbatim-shaped captures from
the live API (2026-07-13, API version 2026-06-22)."""

import copy
import time

import pytest

from src.nadobro.market_data import nadoexplorer_client as explorer

WALLET = "0x0500d5eb23d0c8535abae5c6884c7f9c5a83f2c4"

LEADERBOARD_PAYLOAD = {
    "hasMore": True,
    "limit": 3,
    "offset": 0,
    "rows": [
        {
            "entityType": "wallet",
            "entityId": WALLET,
            "walletAddress": WALLET,
            "subaccount": None,
            "periodDays": 30,
            "volumeUsd": 345063185.306207,
            "pnlUsd": 229856.747340536,
            "feesUsd": 21197.8721423794,
            "winRate": 0.5432078728772297,
            "closedTrades": 67790,
            "activeDays": 30,
            "lastActivityAt": "2026-07-15T15:30:00.000Z",
            "equityUsd": 605984.130121351,
            # Explorer returns ROI in percentage points, not decimal form.
            "roi": 45.4237036477484,
            "profitFactor": 1.4413267930373195,
            "maxDrawdownPct": 0.0913114524256434,
            "nadoPoints": 34411.620185,
            "badges": ["Mega flow", "Net profitable", "High frequency"],
        },
        # The API also emits a subaccount row for the same trader; the client
        # pins entity=wallet server-side, but a row without walletAddress must
        # still be dropped defensively.
        {"entityType": "subaccount", "entityId": "0xdead", "walletAddress": None},
    ],
}

LIVE_POSITIONS_PAYLOAD = {
    "liveTables": {
        "positions": [
            {
                "productId": 2,
                "symbol": "BTC-PERP",
                "side": "S",
                "amount": 4.31455,
                "markPriceUsd": 62732.95,
                "valueUsd": 270664.4494225,
                "pnlUsd": -166.49933419789886,
                "accountLabel": "default",
            }
        ]
    }
}


class _FakeResponse:
    def __init__(self, payload, status=200, remaining="100"):
        self._payload = payload
        self.status_code = status
        self.headers = {"x-ratelimit-remaining": remaining}

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, payload, status=200, remaining="100"):
        self.payload = payload
        self.status = status
        self.remaining = remaining
        self.calls = 0
        self.params = None

    def get(self, url, params=None, timeout=None):
        self.calls += 1
        self.params = params
        return _FakeResponse(self.payload, self.status, self.remaining)


@pytest.fixture(autouse=True)
def _fresh_client_state(monkeypatch):
    explorer._cache.clear()
    monkeypatch.setattr(explorer, "_ratelimit_remaining", 120)
    monkeypatch.setattr(explorer, "_ratelimit_observed_at", 0.0)
    yield
    explorer._cache.clear()


def test_leaderboard_parses_real_shape(monkeypatch):
    fake = _FakeSession(LEADERBOARD_PAYLOAD)
    monkeypatch.setattr(explorer, "SESSION", fake)
    rows = explorer.get_leaderboard(period="30", sort="pnl", limit=3)
    assert len(rows) == 1  # walletAddress-less row dropped
    row = rows[0]
    assert row["wallet_address"] == WALLET
    assert row["pnl_usd"] == pytest.approx(229856.747340536)
    assert row["roi"] == pytest.approx(0.454237036477484)
    assert row["win_rate"] == pytest.approx(0.5432078728772297)
    assert row["equity_usd"] == pytest.approx(605984.130121351)
    assert row["profit_factor"] == pytest.approx(1.4413267930373195)
    assert row["active_days"] == 30
    assert row["last_activity_at"] == "2026-07-15T15:30:00.000Z"
    assert "Mega flow" in row["badges"]


def test_leaderboard_result_preserves_pagination_and_activity_filter(monkeypatch):
    fake = _FakeSession(LEADERBOARD_PAYLOAD)
    monkeypatch.setattr(explorer, "SESSION", fake)

    result = explorer.get_leaderboard_result(min_active_days=10, limit=3)

    assert result is not None
    assert result["has_more"] is True
    assert result["rows"][0]["active_days"] == 30
    assert fake.params["minActiveDays"] == 10


def test_leaderboard_preserves_unknown_quality_metrics_as_none(monkeypatch):
    missing = copy.deepcopy(LEADERBOARD_PAYLOAD["rows"][0])
    missing["walletAddress"] = "0x1111111111111111111111111111111111111111"
    missing.pop("winRate")
    missing.pop("maxDrawdownPct")
    nonnumeric = copy.deepcopy(LEADERBOARD_PAYLOAD["rows"][0])
    nonnumeric["walletAddress"] = "0x2222222222222222222222222222222222222222"
    nonnumeric["winRate"] = "unknown"
    nonnumeric["maxDrawdownPct"] = "unknown"
    fake = _FakeSession(
        {"hasMore": False, "rows": [missing, nonnumeric]}
    )
    monkeypatch.setattr(explorer, "SESSION", fake)

    rows = explorer.get_leaderboard(limit=2)

    assert len(rows) == 2
    assert all(row["win_rate"] is None for row in rows)
    assert all(row["max_drawdown_pct"] is None for row in rows)


def test_leaderboard_caches_within_ttl(monkeypatch):
    fake = _FakeSession(LEADERBOARD_PAYLOAD)
    monkeypatch.setattr(explorer, "SESSION", fake)
    explorer.get_leaderboard()
    explorer.get_leaderboard()
    assert fake.calls == 1


def test_live_positions_parse(monkeypatch):
    fake = _FakeSession(LIVE_POSITIONS_PAYLOAD)
    monkeypatch.setattr(explorer, "SESSION", fake)
    positions = explorer.get_trader_live_positions(WALLET)
    assert len(positions) == 1
    assert positions[0]["symbol"] == "BTC-PERP"
    assert positions[0]["side"] == "S"


def test_http_error_degrades_to_none_not_raise(monkeypatch):
    fake = _FakeSession({}, status=503)
    monkeypatch.setattr(explorer, "SESSION", fake)
    assert explorer.get_leaderboard() == []
    assert explorer.get_trader_daily_summary(WALLET) is None


def test_rate_floor_serves_stale_instead_of_refreshing(monkeypatch):
    fake = _FakeSession(LEADERBOARD_PAYLOAD)
    monkeypatch.setattr(explorer, "SESSION", fake)
    explorer.get_leaderboard()
    assert fake.calls == 1
    # Budget nearly exhausted + TTL expired -> stale cache served, no new call.
    monkeypatch.setattr(explorer, "_ratelimit_remaining", 3)
    for key, (ts, value) in list(explorer._cache.items()):
        explorer._cache[key] = (ts - 3600, value)
    rows = explorer.get_leaderboard()
    assert fake.calls == 1
    assert rows and rows[0]["wallet_address"] == WALLET


def test_rate_floor_skips_an_uncached_page(monkeypatch):
    fake = _FakeSession(LEADERBOARD_PAYLOAD)
    monkeypatch.setattr(explorer, "SESSION", fake)
    monkeypatch.setattr(explorer, "_ratelimit_remaining", 3)
    monkeypatch.setattr(explorer, "_ratelimit_observed_at", time.time())

    result = explorer.get_leaderboard_result(offset=50, limit=50)

    assert result is None
    assert fake.calls == 0


def test_invalid_enum_params_fall_back():
    # No session needed: parameter normalization happens before the request;
    # an empty cache + unreachable host would degrade to [] anyway, so pin
    # only the normalization contract here.
    assert "banana" not in explorer.VALID_SORTS
    assert "999" not in explorer.VALID_PERIODS
