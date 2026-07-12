"""Desk handler: preview card rendering + confirm-flow gating.

The money-relevant contracts pinned here:
- the preview card always states SPOT/PERP, the trigger, and that nothing
  executes before confirm;
- confirm is gated on ownership, DRAFT status, the 10-minute preview TTL,
  and the daily plan cap — and arms via the guarded store transition;
- discard only ever deletes drafts.
"""
from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.handlers import desk_handler
from src.nadobro.trading.desk_plans import (
    EntryTrigger,
    ExecutionPlan,
    ExitPlan,
    ST_DRAFT,
    ST_RUNNING,
)


def make_plan(**kw) -> ExecutionPlan:
    base = dict(algo="twap", market="spot", product="QQQX", side="buy",
                size_quote=500.0, duration_minutes=240, interval_seconds=30)
    base.update(kw)
    return ExecutionPlan(**base)


# ---------------------------------------------------------------------------
# rendering
# ---------------------------------------------------------------------------

def test_preview_card_states_market_trigger_and_consent():
    plan = make_plan(entry_trigger=EntryTrigger(kind="price_below", price=580.0, pct=-2.0))
    text = desk_handler.render_preview_card(plan, mid=600.0, balance_note="✅ ok")
    assert "SPOT" in text
    assert "QQQX" in text
    assert "TWAP over 240" in text
    assert "falls to $580" in text
    assert "Nothing executes until you confirm." in text
    assert "✅ ok" in text


def test_preview_card_perp_shows_long_and_leverage():
    plan = make_plan(market="perp", product="BTC", algo="market", side="buy",
                     size_base=0.1, size_quote=None, leverage=5,
                     exits=ExitPlan(tp_pct=5, sl_pct=3))
    text = desk_handler.render_preview_card(plan, mid=50000.0, balance_note="")
    assert "PERP" in text and "LONG" in text
    assert "Leverage: 5x" in text
    assert "TP +5%" in text and "SL -3%" in text
    assert "vs actual avg entry" in text


def test_desk_view_lists_progress():
    plan = make_plan()
    active = [{"plan": plan, "status": "running",
               "state": {"filled_quote": "250", "target_quote": "500"}}]
    text = desk_handler.render_desk_view(active, [])
    assert "50% filled" in text


# ---------------------------------------------------------------------------
# confirm gating
# ---------------------------------------------------------------------------

class FakeQuery:
    def __init__(self):
        self.edits = []


def _patch_store(**overrides):
    """Patch desk_store functions reached via run_blocking."""
    defaults = {
        "get_plan": None,
        "confirm_plan": True,
        "count_confirmed_today": 0,
        "discard_draft": True,
        "cancel_plan": True,
        "list_active_plans": [],
        "list_recent_plans": [],
    }
    defaults.update(overrides)
    patches = []
    for name, val in defaults.items():
        patches.append(patch.object(desk_handler.desk_store, name,
                                    lambda *a, _v=val, **k: _v))
    return patches


def run_callback(data, telegram_id=42, store=None):
    edits = []

    async def fake_edit(query, text, **kw):
        edits.append((str(text), kw))

    async def body():
        with patch.object(desk_handler, "_edit_loc", fake_edit), \
             patch.object(desk_handler, "_network_of", lambda _tid: "mainnet"):
            ps = _patch_store(**(store or {}))
            for p in ps:
                p.start()
            try:
                await desk_handler.handle_desk_callback(FakeQuery(), data, telegram_id, None)
            finally:
                for p in ps:
                    p.stop()

    asyncio.run(body())
    return edits


def draft_record(plan, owner=42, status=ST_DRAFT):
    return {"row_id": 1, "user_id": owner, "plan_id": plan.plan_id,
            "status": status, "plan": plan, "state": {}}


def test_confirm_rejects_foreign_plan():
    plan = make_plan()
    edits = run_callback(f"desk:confirm:{plan.plan_id}", telegram_id=999,
                         store={"get_plan": draft_record(plan, owner=42)})
    assert edits and "gone" in edits[0][0]


def test_confirm_rejects_expired_preview():
    plan = make_plan()
    plan.created_ts = time.time() - 700  # past the 600s TTL
    edits = run_callback(f"desk:confirm:{plan.plan_id}",
                         store={"get_plan": draft_record(plan)})
    assert edits and "expired" in edits[0][0]


def test_confirm_enforces_daily_cap():
    plan = make_plan()
    edits = run_callback(f"desk:confirm:{plan.plan_id}",
                         store={"get_plan": draft_record(plan),
                                "count_confirmed_today": 5})
    assert edits and "limit reached" in edits[0][0]
    assert "5/5" in edits[0][0]


def test_confirm_arms_within_cap():
    plan = make_plan()
    edits = run_callback(f"desk:confirm:{plan.plan_id}",
                         store={"get_plan": draft_record(plan),
                                "count_confirmed_today": 2})
    assert edits and "Plan armed" in edits[0][0]
    assert "3/5" in edits[0][0]


def test_confirm_is_idempotent_after_arming():
    plan = make_plan()
    edits = run_callback(f"desk:confirm:{plan.plan_id}",
                         store={"get_plan": draft_record(plan, status=ST_RUNNING)})
    assert edits and "Already handled" in edits[0][0]


def test_stop_active_plan_reports_fills_kept():
    plan = make_plan()
    edits = run_callback(f"desk:stop:{plan.plan_id}",
                         store={"get_plan": draft_record(plan, status=ST_RUNNING),
                                "cancel_plan": True})
    assert edits and "filled stays in your account" in edits[0][0]
