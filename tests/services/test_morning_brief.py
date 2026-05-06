import asyncio

import pytest

from src.nadobro.connectors.news import NewsItem
from src.nadobro.services import market_snapshot, morning_brief, news_aggregator
from src.nadobro.services.market_snapshot import SnapshotPayload, SnapshotRow


@pytest.fixture(autouse=True)
def _clear_caches():
    morning_brief.clear_cache()
    news_aggregator.clear_cache()
    market_snapshot.clear_cache()
    yield
    morning_brief.clear_cache()
    news_aggregator.clear_cache()
    market_snapshot.clear_cache()


def _stub_snapshot(*_args, **_kwargs):
    async def _impl(*a, **kw):
        return SnapshotPayload(
            rows=[
                SnapshotRow(
                    symbol="BTC",
                    mid=81267.55,
                    bid=81260.0,
                    ask=81275.0,
                    spread_bps=1.8,
                    funding_rate=0.0001,
                    change_24h_pct=2.4,
                    volume_24h_usd=1_200_000_000,
                    open_interest=890_000_000,
                    high_24h=81600.0,
                    low_24h=78900.0,
                ),
                SnapshotRow(
                    symbol="ETH",
                    mid=2364.12,
                    bid=2363.5,
                    ask=2364.8,
                    spread_bps=2.5,
                    funding_rate=0.00008,
                    change_24h_pct=1.1,
                    volume_24h_usd=600_000_000,
                    open_interest=320_000_000,
                    high_24h=2400.0,
                    low_24h=2310.0,
                ),
            ],
            fear_greed_value=72,
            fear_greed_label="Greed",
            network="mainnet",
            generated_at=0.0,
        )

    return _impl()


def _stub_news_bundle():
    items = [
        NewsItem(title="Bitcoin tops 80K on ETF inflows", url="https://reuters.com/x", source="Reuters", category="crypto"),
        NewsItem(title="Fed signals patient stance on rate cuts", url="https://federalreserve.gov/y", source="Federal Reserve", category="economics"),
    ]
    return news_aggregator.NewsBundle(
        items=items,
        by_category={"crypto": items[:1], "economics": items[1:]},
        sources_used=["Reuters", "Federal Reserve"],
        generated_at=0.0,
    )


def test_brief_renders_with_canned_grok_json(monkeypatch):
    monkeypatch.setattr(market_snapshot, "gather_snapshot", _stub_snapshot)

    async def _fake_news(**_kwargs):
        return _stub_news_bundle()

    monkeypatch.setattr(news_aggregator, "fetch_news_bundle", _fake_news)

    canned = {
        "snapshot_lines": [
            "BTC: $81,267.55 — Holding above $80K with steady ETF inflows.",
            "ETH: $2,364.12 — Drifts with BTC; funding stays modest.",
        ],
        "news_drivers": [
            {"title": "Bitcoin tops 80K on ETF inflows.", "tag": "Reuters", "category": "crypto"},
            {"title": "Fed signals patient stance on rate cuts.", "tag": "Federal Reserve", "category": "economics"},
        ],
        "insight": "Risk-on tilts but stay disciplined — BTC perp on Nado around $81K is the cleanest exposure.",
    }

    def fake_chat_json(messages, schema=None, model=None):
        return canned, "grok"

    import src.nadobro.services.bro_llm as bro_llm
    monkeypatch.setattr(bro_llm, "chat_json", fake_chat_json)

    text, sources = asyncio.run(morning_brief.render_morning_brief(network="mainnet"))
    assert "Market Snapshot" in text
    assert "Key News Drivers" in text
    assert "Actionable Insight" in text
    assert "Reuters" in sources or "Reuters" in text
    assert text.count("\n") < 25


def test_brief_handles_enum_network(monkeypatch):
    """Regression: prod passed user.network_mode (a NetworkMode enum) into render.
    json.dumps blew up with 'Object of type NetworkMode is not JSON serializable'.
    """
    import enum

    class FakeNetworkMode(enum.Enum):
        MAINNET = "mainnet"
        TESTNET = "testnet"

    async def _enum_snap(network, **_kwargs):
        snap = (await _stub_snapshot())
        snap.network = FakeNetworkMode.MAINNET  # simulate enum leaking to SnapshotPayload
        return snap

    monkeypatch.setattr(market_snapshot, "gather_snapshot", _enum_snap)

    async def _fake_news(**_kwargs):
        return _stub_news_bundle()

    monkeypatch.setattr(news_aggregator, "fetch_news_bundle", _fake_news)

    def fake_chat_json(messages, schema=None, model=None):
        return {"snapshot_lines": ["BTC: $1"], "news_drivers": [], "insight": "ok"}, "grok"

    import src.nadobro.services.bro_llm as bro_llm
    monkeypatch.setattr(bro_llm, "chat_json", fake_chat_json)

    # Before fix this raised: TypeError: Object of type FakeNetworkMode is not JSON serializable
    text, _ = asyncio.run(morning_brief.render_morning_brief(network=FakeNetworkMode.MAINNET))
    assert "Market Snapshot" in text


def test_brief_falls_back_when_llm_fails(monkeypatch):
    monkeypatch.setattr(market_snapshot, "gather_snapshot", _stub_snapshot)

    async def _fake_news(**_kwargs):
        return _stub_news_bundle()

    monkeypatch.setattr(news_aggregator, "fetch_news_bundle", _fake_news)

    def fake_chat_json(messages, schema=None, model=None):
        raise RuntimeError("no key")

    import src.nadobro.services.bro_llm as bro_llm
    monkeypatch.setattr(bro_llm, "chat_json", fake_chat_json)

    text, _ = asyncio.run(morning_brief.render_morning_brief(network="mainnet"))
    assert "Market Snapshot" in text
    assert "BTC" in text
