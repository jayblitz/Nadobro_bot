import asyncio

import pytest

from src.nadobro.connectors.news import NewsItem
from src.nadobro.services import news_aggregator


@pytest.fixture(autouse=True)
def _clear_cache():
    news_aggregator.clear_cache()
    yield
    news_aggregator.clear_cache()


def test_bundle_empty_when_all_connectors_return_empty(monkeypatch):
    new_connectors = [(lambda limit, _orig=fn: [], cat) for fn, cat in news_aggregator._CONNECTORS]
    monkeypatch.setattr(news_aggregator, "_CONNECTORS", new_connectors)

    bundle = asyncio.run(news_aggregator.fetch_news_bundle())
    assert bundle.items == []
    assert bundle.by_category == {}
    assert bundle.sources_used == []


def test_bundle_dedupes_and_groups(monkeypatch):
    item_a = NewsItem(title="BTC tops 80K", url="https://a.com/x", source="A", category="crypto", published_at=100.0)
    item_b = NewsItem(title="BTC tops 80K", url="https://a.com/x", source="A", category="crypto", published_at=110.0)
    item_c = NewsItem(title="Fed signals cut", url="https://b.com/y", source="B", category="economics", published_at=120.0)
    fake_connectors = [
        (lambda limit: [item_a, item_b], "crypto"),
        (lambda limit: [item_c], "economics"),
    ]
    monkeypatch.setattr(news_aggregator, "_CONNECTORS", fake_connectors)

    bundle = asyncio.run(news_aggregator.fetch_news_bundle())
    titles = [it.title for it in bundle.items]
    assert titles.count("BTC tops 80K") == 1
    assert "Fed signals cut" in titles
    assert "crypto" in bundle.by_category
    assert "economics" in bundle.by_category
    assert set(bundle.sources_used) == {"A", "B"}


def test_bundle_uses_cache(monkeypatch):
    call_count = {"n": 0}

    def fake_fetch(limit):
        call_count["n"] += 1
        return [NewsItem(title=f"hit {call_count['n']}", url="https://x.com", source="Z", category="crypto")]

    monkeypatch.setattr(news_aggregator, "_CONNECTORS", [(fake_fetch, "crypto")])

    asyncio.run(news_aggregator.fetch_news_bundle())
    asyncio.run(news_aggregator.fetch_news_bundle())
    assert call_count["n"] == 1


def test_bundle_filters_by_category(monkeypatch):
    crypto = NewsItem(title="c", url="https://c.x", source="C", category="crypto")
    econ = NewsItem(title="e", url="https://e.x", source="E", category="economics")
    fake_connectors = [
        (lambda limit: [crypto], "crypto"),
        (lambda limit: [econ], "economics"),
    ]
    monkeypatch.setattr(news_aggregator, "_CONNECTORS", fake_connectors)

    bundle = asyncio.run(news_aggregator.fetch_news_bundle(categories=["economics"]))
    titles = [it.title for it in bundle.items]
    assert titles == ["e"]
