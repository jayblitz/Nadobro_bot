import asyncio

from src.nadobro.engine.routines import market_scanner as ms


def test_ranking_top_n():
    cands = [
        {"pair": "A", "liquidity": 100, "volume": 100, "volatility": 1},
        {"pair": "B", "liquidity": 10, "volume": 10, "volatility": 10},
        {"pair": "C", "liquidity": 1, "volume": 1, "volatility": 1},
    ]
    out = asyncio.run(ms.run(cands, top_n=2))
    assert len(out) == 2
    assert out[0]["pair"] == "A"
    assert "reason" in out[0] and out[0]["score"] >= out[1]["score"]


def test_empty_universe():
    assert asyncio.run(ms.run([])) == []
