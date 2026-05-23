import asyncio
from decimal import Decimal

from src.nadobro.engine.routines import funding_scan as fs


def test_scan_and_extremes():
    rates = {"A": Decimal("-0.01"), "B": Decimal("0.02"), "C": Decimal("0")}
    out = asyncio.run(fs.run(["A", "B", "C"], lambda p: rates[p]))
    assert out == rates
    assert fs.most_negative(out) == "A"
    assert fs.most_positive(out) == "B"


def test_async_provider_skips_none():
    async def prov(p):
        return {"A": Decimal("0.01")}.get(p)

    out = asyncio.run(fs.run(["A", "B"], prov))
    assert out == {"A": Decimal("0.01")}
    assert fs.most_negative({}) is None and fs.most_positive({}) is None
