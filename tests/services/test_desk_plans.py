"""Desk plan validation matrix + trigger resolution semantics.

validate_plan is the deterministic gate the LLM cannot cross: every plan —
regex- or LLM-parsed — passes through it before the preview card, and again
at confirm. Trigger resolution pins %-moves and directionless levels to the
arrival mid the user saw, so the runtime watcher is a dumb absolute check.
"""
from __future__ import annotations

import pytest

from src.nadobro.services.desk_plans import (
    EntryTrigger,
    ExecutionPlan,
    ExitPlan,
    daily_plan_cap,
    describe_trigger,
    resolve_trigger,
    trigger_satisfied,
    validate_plan,
)

PERPS = {"BTC", "ETH", "SOL"}
SPOTS = {"BTC", "ETH", "QQQX"}


def make_plan(**kw) -> ExecutionPlan:
    base = dict(algo="market", market="spot", product="ETH", side="buy", size_base=1.0)
    base.update(kw)
    return ExecutionPlan(**base)


def problems(plan, **kw):
    kw.setdefault("perp_symbols", PERPS)
    kw.setdefault("spot_symbols", SPOTS)
    return validate_plan(plan, **kw)


def test_valid_simple_spot_buy():
    assert problems(make_plan()) == []


def test_valid_perp_twap_with_exits():
    plan = make_plan(
        algo="twap", market="perp", product="BTC", leverage=3,
        duration_minutes=120, interval_seconds=30,
        exits=ExitPlan(tp_pct=5, sl_pct=3),
    )
    assert problems(plan, max_leverage=10) == []


# -- catalog routing ---------------------------------------------------------

def test_spot_only_token_rejects_perp_with_guidance():
    plan = make_plan(market="perp", product="QQQX")
    msgs = problems(plan)
    assert any("no perp market" in m for m in msgs)


def test_perp_only_token_rejects_spot_with_guidance():
    plan = make_plan(market="spot", product="SOL")
    msgs = problems(plan)
    assert any("no spot market" in m for m in msgs)


def test_unknown_product():
    assert any("Unknown spot product" in m for m in problems(make_plan(product="DOGE")))


# -- size --------------------------------------------------------------------

def test_missing_size():
    assert any("Missing size" in m for m in problems(make_plan(size_base=None)))


def test_both_sizes_rejected():
    plan = make_plan(size_quote=100.0)
    assert any("not both" in m for m in problems(plan))


def test_min_notional_with_mid():
    plan = make_plan(size_base=0.001)
    assert any("minimum size" in m for m in problems(plan, mid_price=100.0))
    assert problems(plan, mid_price=100000.0) == []


# -- market rules ------------------------------------------------------------

def test_spot_leverage_rejected():
    msgs = problems(make_plan(leverage=5))
    assert any("Spot has no leverage" in m for m in msgs)


def test_perp_leverage_cap():
    plan = make_plan(market="perp", product="BTC", leverage=50)
    assert any("Max leverage" in m for m in problems(plan, max_leverage=20))


def test_spot_trailing_rejected():
    plan = make_plan(exits=ExitPlan(trailing_pct=2.0))
    assert any("perp-only" in m for m in problems(plan))


def test_spot_sell_with_exits_rejected():
    plan = make_plan(side="sell", exits=ExitPlan(tp_pct=5))
    assert any("IS the exit" in m for m in problems(plan))


# -- twap rules ---------------------------------------------------------------

def test_twap_duration_bounds():
    too_short = make_plan(algo="twap", duration_minutes=1, interval_seconds=30)
    assert any("at least 2 minutes" in m for m in problems(too_short))
    too_long = make_plan(algo="twap", duration_minutes=8 * 24 * 60, interval_seconds=600)
    assert any("at most 7 days" in m for m in problems(too_long))


def test_twap_slice_cap():
    plan = make_plan(algo="twap", duration_minutes=7 * 24 * 60, interval_seconds=10)
    assert any("slices" in m for m in problems(plan))


def test_twap_with_limit_price_rejected():
    plan = make_plan(algo="twap", duration_minutes=60, interval_seconds=30, limit_price=100.0)
    assert any("don't combine" in m for m in problems(plan))


# -- triggers ------------------------------------------------------------------

def test_trigger_validation():
    bad_pct = make_plan(entry_trigger=EntryTrigger(kind="pct_move", pct=80))
    assert any("±50%" in m for m in problems(bad_pct))
    no_price = make_plan(entry_trigger=EntryTrigger(kind="price_below"))
    assert any("positive price" in m for m in problems(no_price))
    bad_kind = make_plan(entry_trigger=EntryTrigger(kind="spread"))
    assert any("Unsupported trigger" in m for m in problems(bad_kind))


def test_resolve_pct_move_anchors_to_arrival_mid():
    t = resolve_trigger(EntryTrigger(kind="pct_move", pct=-2.0), arrival_mid=1000.0)
    assert t.kind == "price_below"
    assert t.price == pytest.approx(980.0)
    up = resolve_trigger(EntryTrigger(kind="pct_move", pct=2.0), arrival_mid=1000.0)
    assert up.kind == "price_above"
    assert up.price == pytest.approx(1020.0)


def test_resolve_price_cross_picks_direction_from_mid():
    above = resolve_trigger(EntryTrigger(kind="price_cross", price=1100.0), arrival_mid=1000.0)
    assert above.kind == "price_above"
    below = resolve_trigger(EntryTrigger(kind="price_cross", price=900.0), arrival_mid=1000.0)
    assert below.kind == "price_below"


def test_resolve_time_sets_fire_at():
    t = resolve_trigger(EntryTrigger(kind="time", delay_minutes=30), arrival_mid=1.0, now=1000.0)
    assert t.fire_at_ts == 1000.0 + 30 * 60


def test_trigger_satisfied_runtime_checks():
    assert trigger_satisfied(None, mid=1.0)  # no trigger == start now
    below = EntryTrigger(kind="price_below", price=980.0)
    assert not trigger_satisfied(below, mid=990.0)
    assert trigger_satisfied(below, mid=979.0)
    above = EntryTrigger(kind="price_above", price=1020.0)
    assert not trigger_satisfied(above, mid=1019.0)
    assert trigger_satisfied(above, mid=1021.0)
    timed = EntryTrigger(kind="time", fire_at_ts=2000.0)
    assert not trigger_satisfied(timed, mid=1.0, now=1999.0)
    assert trigger_satisfied(timed, mid=1.0, now=2001.0)


def test_unresolved_pct_trigger_never_fires_silently():
    # A pct_move that skipped confirm-time resolution must NOT fire.
    assert not trigger_satisfied(EntryTrigger(kind="pct_move", pct=-2.0), mid=1.0)


def test_describe_trigger_human_strings():
    assert describe_trigger(None) == "immediately"
    assert "dumps 2" in describe_trigger(EntryTrigger(kind="pct_move", pct=-2.0))
    assert "$2,500" in describe_trigger(EntryTrigger(kind="price_cross", price=2500.0))


# -- misc ----------------------------------------------------------------------

def test_daily_cap_default_and_env(monkeypatch):
    monkeypatch.delenv("NADO_DESK_DAILY_PLAN_CAP", raising=False)
    assert daily_plan_cap() == 5
    monkeypatch.setenv("NADO_DESK_DAILY_PLAN_CAP", "9")
    assert daily_plan_cap() == 9
    monkeypatch.setenv("NADO_DESK_DAILY_PLAN_CAP", "junk")
    assert daily_plan_cap() == 5
