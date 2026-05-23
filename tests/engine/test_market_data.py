import asyncio
from decimal import Decimal

from src.nadobro.engine.market_data import MarketData


class _StubAdapter:
    def __init__(self):
        self.candle_calls = 0
        self.mid_calls = 0
        self.funding_calls = 0

    async def mid_price(self, pair):
        self.mid_calls += 1
        return Decimal(100)

    async def candles(self, pair, timeframe="1h", limit=200):
        self.candle_calls += 1
        return [{"close": 100.0 + i} for i in range(3)]

    async def funding_rate(self, pair):
        self.funding_calls += 1
        return Decimal("0.0001")


def test_caches_within_ttl_and_invalidate_refetches():
    async def body():
        a = _StubAdapter()
        md = MarketData(a, ttl_seconds=60)
        c1 = await md.candles("SOL-USDC")
        c2 = await md.candles("SOL-USDC")
        assert c1 == c2 and a.candle_calls == 1     # second served from cache
        md.invalidate()
        await md.candles("SOL-USDC")
        assert a.candle_calls == 2                  # refetched after invalidate

    asyncio.run(body())


def test_mid_and_funding_and_distinct_keys():
    async def body():
        a = _StubAdapter()
        md = MarketData(a, ttl_seconds=60)
        assert await md.mid("SOL-USDC") == Decimal(100)
        assert await md.funding("SOL-USDC") == Decimal("0.0001")
        # different timeframe is a distinct cache key
        await md.candles("SOL-USDC", "1h")
        await md.candles("SOL-USDC", "5m")
        assert a.candle_calls == 2

    asyncio.run(body())


def test_provider_callables():
    async def body():
        a = _StubAdapter()
        md = MarketData(a, ttl_seconds=60)
        cp = md.candle_provider(timeframe="15m", limit=50)
        candles = await cp("SOL-USDC")
        assert len(candles) == 3 and a.candle_calls == 1
        fp = md.funding_provider()
        assert await fp("SOL-USDC") == Decimal("0.0001")

    asyncio.run(body())
