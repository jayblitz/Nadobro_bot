"""Portfolio handler v2: per-controller PnL rendering from engine.portfolio."""
from __future__ import annotations

from decimal import Decimal

from src.nadobro.engine.portfolio import ControllerPnL, PortfolioState
from src.nadobro.handlers.portfolio_deck import render_per_controller_pnl


def test_render_per_controller_pnl_section():
    state = PortfolioState(
        per_controller={
            "grid-1": ControllerPnL(
                realized=Decimal(40),
                unrealized=Decimal(120),
                fees=Decimal(2),
                net=Decimal(158),
                open_executors=2,
            )
        }
    )
    text = render_per_controller_pnl(state)
    assert "Strategy PnL" in text
    assert "grid-1" in text
    assert "158" in text
    assert "2 open" in text


def test_render_empty_when_no_controllers():
    assert render_per_controller_pnl(PortfolioState()) == ""
