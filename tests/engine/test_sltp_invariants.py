"""Executable SL/TP & strategy-config invariants — the self-review guardrails.

This file is the machine-checkable expression of the 2026-06-20 strategy audit
(``docs/audit/STRATEGY_SLTP_AUDIT_2026-06-20.md``). Each test encodes ONE
invariant the trading strategies must satisfy so a user's configured SL/TP and
sizing are actually honored and the bot does not bleed money.

Two kinds of tests live here:

* **Green invariants** — properties that hold today. They guard against
  regression (e.g. the rgrid/dgrid SL/TP key resolution that was already fixed).
* **xfail invariants** — known-broken properties from the audit, marked
  ``@pytest.mark.xfail(strict=True)`` with the audit ID in the reason. When the
  underlying bug is fixed the test XPASSes and ``strict=True`` turns that into a
  CI failure — your signal to delete the xfail marker and lock the fix in.

Run just these::

    python -m pytest tests/engine/test_sltp_invariants.py -v

No DB or network required — these exercise pure config/resolution logic.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from src.nadobro.services.engine_runtime import (
    ENGINE_MAPPED_STRATEGIES,
    map_strategy_config,
)
from src.nadobro.services.strategy_registry import effective_sl_tp_pct

MID = Decimal("100")
PRODUCT = "BTC-PERP"


# --------------------------------------------------------------------------- #
# Green invariants — must always hold (guard against regression)              #
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize(
    "strategy,conf,expected",
    [
        ("grid", {"sl_pct": 0.5, "tp_pct": 1.0}, (0.5, 1.0)),
        ("mid", {"sl_pct": 0.3, "tp_pct": 0.7}, (0.3, 0.7)),
        # rgrid/dgrid store SL/TP under the rgrid_* keys the UI writes.
        ("rgrid", {"rgrid_stop_loss_pct": 0.8, "rgrid_take_profit_pct": 1.2}, (0.8, 1.2)),
        ("dgrid", {"rgrid_stop_loss_pct": 0.8, "rgrid_take_profit_pct": 1.2}, (0.8, 1.2)),
        ("dn", {"sl_pct": 0.6, "tp_pct": 0.8}, (0.6, 0.8)),
    ],
)
def test_user_sltp_is_resolved_to_the_field_the_user_actually_wrote(strategy, conf, expected):
    """A user's configured SL/TP must resolve back out, per strategy.

    Guards the rgrid/dgrid key-name fix (audit: 'clean / not a bug').
    """
    assert effective_sl_tp_pct(strategy, conf) == expected


def test_dgrid_falls_back_to_sl_pct_when_rgrid_keys_absent():
    """dgrid/rgrid must fall back to sl_pct/tp_pct if the rgrid_* keys are unset."""
    assert effective_sl_tp_pct("dgrid", {"sl_pct": 0.4, "tp_pct": 0.9}) == (0.4, 0.9)


def test_every_engine_strategy_resolves_some_sltp_without_crashing():
    """effective_sl_tp_pct must be total over the supported strategy set."""
    for strategy in ENGINE_MAPPED_STRATEGIES:
        sl, tp = effective_sl_tp_pct(strategy, {"sl_pct": 1.0, "tp_pct": 2.0})
        assert isinstance(sl, float) and isinstance(tp, float)


# --------------------------------------------------------------------------- #
# xfail invariants — known bugs from the audit. Fix the code, then delete the  #
# marker (strict=True makes an unexpected pass fail CI).                       #
# --------------------------------------------------------------------------- #

@pytest.mark.xfail(strict=True, reason="VOL-MARGIN: vol ignores session_margin_usd; trades the $100 default")
def test_vol_uses_user_session_margin():
    """A user's vol 'Session margin' must size the run.

    Today map_strategy_config reads cycle_notional_usd/notional_usd only, so
    total_amount_quote stays at the 100.0 default even when the user set 500.
    """
    cfg = map_strategy_config("vol", {"session_margin_usd": 500}, MID, product=PRODUCT)
    assert float(cfg["total_amount_quote"]) == pytest.approx(500.0)


@pytest.mark.xfail(strict=True, reason="VOL-DEAD-SL: vol config carries no SL the controller can act on")
def test_vol_config_carries_user_stop_loss():
    """A vol user's stop-loss must reach the controller config."""
    cfg = map_strategy_config("vol", {"session_margin_usd": 100, "sl_pct": 2.0}, MID, product=PRODUCT)
    assert "sl_pct" in cfg or cfg.get("triple_barrier_config") is not None


@pytest.mark.xfail(
    strict=True,
    reason="GRID-DUAL-UNIT: grid barrier reuses sl_pct as a price-move fraction "
    "AND the rail uses it as % of margin. The barrier should not double-apply "
    "the margin-% stop. Fix: stop_loss=None on the grid barrier, or convert units.",
)
def test_grid_barrier_does_not_double_apply_margin_pct_stop():
    cfg = map_strategy_config("grid", {"sl_pct": 0.5, "tp_pct": 1.0}, MID, product=PRODUCT)
    tbc = cfg.get("triple_barrier_config")
    assert tbc is None or getattr(tbc, "stop_loss", None) is None


# Note on DN-RAIL (Critical) and SLTP-GROSS / GRID-TP-DEAD:
# These live in bot_runtime/live_session/grid_executor and need a running
# session to assert directly. They are tracked as checklist items in
# docs/audit/SELF_REVIEW_WORKFLOW.md and should get dedicated integration tests
# when the fixes land. The cheapest structural guard ships below.

def test_dn_is_an_engine_mapped_strategy_so_a_rail_can_target_it():
    """DN must be a recognized engine strategy (precondition for a session rail).

    This does NOT prove the rail exists (audit DN-RAIL: it does not). It guards
    the precondition; see SELF_REVIEW_WORKFLOW.md checklist item DN-RAIL for the
    integration test to add alongside the fix.
    """
    assert "dn" in ENGINE_MAPPED_STRATEGIES
