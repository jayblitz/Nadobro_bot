"""Position Hold inventory tests, incl. the Condor worked example."""
from __future__ import annotations

from decimal import Decimal

from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.types import PositionSide, TradeType

PAIR = "SOL-USDC"
CID = "ctrl-1"
UID = 1


def test_condor_worked_example():
    repo = InventoryRepository()
    repo.apply_fill(UID, PAIR, CID, TradeType.BUY, Decimal(100), Decimal(100) * Decimal(150))
    repo.apply_fill(UID, PAIR, CID, TradeType.BUY, Decimal(50), Decimal(50) * Decimal(145))
    hold = repo.apply_fill(
        UID, PAIR, CID, TradeType.SELL, Decimal(100), Decimal(100) * Decimal(155)
    )
    assert hold.net_amount_base == Decimal(50)
    assert hold.side is PositionSide.LONG
    assert round(hold.breakeven, 2) == Decimal("148.33")
    assert round(hold.realized_pnl, 2) == Decimal("666.67")
    assert int(round(hold.realized_pnl)) == 667


def test_long_unrealized_pnl():
    repo = InventoryRepository()
    repo.apply_fill(UID, PAIR, CID, TradeType.BUY, Decimal(10), Decimal(1500))  # 10 @150
    hold = repo.get(UID, PAIR, CID)
    assert hold.unrealized_pnl(Decimal(160)) == Decimal(100)
    assert hold.unrealized_pnl(Decimal(140)) == Decimal(-100)


def test_fee_accounting():
    repo = InventoryRepository()
    repo.apply_fill(UID, PAIR, CID, TradeType.BUY, Decimal(10), Decimal(1000), Decimal("0.5"))
    hold = repo.apply_fill(
        UID, PAIR, CID, TradeType.SELL, Decimal(10), Decimal(1100), Decimal("0.6")
    )
    assert hold.cum_fees_quote == Decimal("1.1")
    assert hold.realized_pnl == Decimal(100)
    assert hold.realized_pnl_after_fees == Decimal("98.9")


def test_perp_short_side():
    repo = InventoryRepository()
    repo.apply_fill(UID, PAIR, CID, TradeType.SELL, Decimal(10), Decimal(10) * Decimal(200))
    hold = repo.get(UID, PAIR, CID)
    assert hold.side is PositionSide.SHORT
    assert hold.net_amount_base == Decimal(-10)
    assert hold.breakeven == Decimal(200)
    assert hold.unrealized_pnl(Decimal(190)) == Decimal(100)
    assert hold.unrealized_pnl(Decimal(210)) == Decimal(-100)


def test_flat_position():
    repo = InventoryRepository()
    repo.apply_fill(UID, PAIR, CID, TradeType.BUY, Decimal(5), Decimal(500))
    repo.apply_fill(UID, PAIR, CID, TradeType.SELL, Decimal(5), Decimal(550))
    hold = repo.get(UID, PAIR, CID)
    assert hold.side is PositionSide.FLAT
    assert hold.breakeven is None
    assert hold.unrealized_pnl(Decimal(999)) == Decimal(0)
    assert hold.realized_pnl == Decimal(50)


def test_multi_controller_isolation_same_pair():
    repo = InventoryRepository()
    repo.apply_fill(UID, PAIR, "c1", TradeType.BUY, Decimal(10), Decimal(1000))
    repo.apply_fill(UID, PAIR, "c2", TradeType.SELL, Decimal(5), Decimal(600))
    h1 = repo.get(UID, PAIR, "c1")
    h2 = repo.get(UID, PAIR, "c2")
    assert h1.net_amount_base == Decimal(10)
    assert h2.net_amount_base == Decimal(-5)
    assert len(repo.list_for_user(UID)) == 2
    assert len(repo.list_for_controller(UID, "c1")) == 1


def test_negative_fill_rejected():
    repo = InventoryRepository()
    try:
        repo.apply_fill(UID, PAIR, CID, TradeType.BUY, Decimal(-1), Decimal(1))
    except ValueError:
        return
    raise AssertionError("expected ValueError for negative fill")
