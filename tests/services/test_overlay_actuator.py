"""Overlay actuator — bounded signal→config mapping and the drawdown kill-switch."""
from __future__ import annotations

from decimal import Decimal

import pytest

from src.nadobro.strategy import overlay_actuator as oa
from src.nadobro.llm.signal_engine import Signal


def test_drawdown_kill_switch():
    assert oa.overlay_drawdown_breached(-10.01) is True
    assert oa.overlay_drawdown_breached(-10.0) is True
    assert oa.overlay_drawdown_breached(-9.99) is False
    assert oa.overlay_drawdown_breached(5.0) is False
    # custom cap
    assert oa.overlay_drawdown_breached(-6.0, cap_pct=5.0) is True


def test_overlay_applies_only_to_mm(monkeypatch):
    monkeypatch.setenv("NADO_SIGNAL_OVERLAY", "1")
    for s in ("grid", "rgrid", "dgrid", "mid"):
        assert oa.overlay_applies(s)
    for s in ("dn", "vol", "copy", "bro"):
        assert not oa.overlay_applies(s)


def test_overlay_disabled_by_flag(monkeypatch):
    monkeypatch.setenv("NADO_SIGNAL_OVERLAY", "0")
    assert oa.overlay_applies("grid") is False


def test_mid_bias_and_size_scale_up_on_strong_trend():
    sig = Signal(bias=0.7, regime="trend_up", entry_ok=True, scale=0.6, spread_mult=1.4, confidence=0.85)
    ov = oa.compute_overrides("mid", sig)
    assert ov["directional_bias"] == 0.7
    cfg = {"order_amount_quote": Decimal("500"), "spread_bid_pct": Decimal("0.0005"),
           "spread_ask_pct": Decimal("0.0005"), "directional_bias": 0.0}
    oa.apply_overrides_to_configs("mid", cfg, ov)
    assert cfg["directional_bias"] == 0.7
    assert Decimal(cfg["order_amount_quote"]) > Decimal("500")   # added on the trend
    assert Decimal(cfg["spread_bid_pct"]) > Decimal("0.0005")    # widened


def test_size_and_spread_bounds_never_exceeded():
    sig = Signal(bias=1.0, regime="trend_up", entry_ok=True, scale=1.0, spread_mult=99.0, confidence=1.0)
    ov = oa.compute_overrides("grid", sig)
    assert ov["size_factor"] <= 1.25
    assert ov["spread_factor"] <= 3.0
    neg = Signal(bias=-1.0, regime="trend_down", entry_ok=True, scale=-1.0, spread_mult=0.01, confidence=1.0)
    ovn = oa.compute_overrides("grid", neg)
    assert ovn["size_factor"] >= 0.5
    assert ovn["spread_factor"] >= 0.75


def test_chop_suppresses_new_exposure():
    sig = Signal(bias=0.05, regime="chop", entry_ok=False, scale=0.0, spread_mult=1.0, confidence=0.2)
    ov = oa.compute_overrides("grid", sig)
    assert ov["suppress_new_entries"] is True
    cfg = {"total_amount_quote": Decimal("400"), "max_net_exposure_pct": 30.0}
    oa.apply_overrides_to_configs("grid", cfg, ov)
    assert cfg["max_net_exposure_pct"] == 0.0          # exposure cap chokes adds
    assert cfg["regime_gate_enabled"] is True


def test_suppress_never_also_adds_size():
    sig = Signal(bias=0.8, regime="chop", entry_ok=False, scale=0.9, spread_mult=1.0, confidence=0.9)
    ov = oa.compute_overrides("mid", sig)
    assert ov["suppress_new_entries"] is True
    assert ov["size_factor"] <= 1.0


def test_spread_fee_floor_enforced():
    cfg = {"spread_bid_pct": Decimal("0.00002"), "spread_ask_pct": Decimal("0.00002")}
    oa.apply_overrides_to_configs(
        "mid", cfg,
        {"size_factor": 1.0, "spread_factor": 0.75, "suppress_new_entries": False},
    )
    assert Decimal(cfg["spread_bid_pct"]) >= Decimal("0.00015")
    assert Decimal(cfg["spread_ask_pct"]) >= Decimal("0.00015")


def test_grid_gets_no_directional_bias_key():
    # directional_bias is a Mid-only continuous knob.
    ov = oa.compute_overrides("grid", Signal(bias=0.5, regime="trend_up", entry_ok=True, scale=0.3))
    assert "directional_bias" not in ov


def test_signal_barriers_flow_into_overrides_and_grid_barrier():
    from src.nadobro.engine.types import TripleBarrierConfig
    sig = Signal(bias=0.7, regime="trend_up", entry_ok=True, scale=0.5,
                 spread_mult=1.2, confidence=0.8, sl_pct=0.65, tp_pct=1.6)
    ov = oa.compute_overrides("grid", sig)
    assert ov["sl_pct"] == 0.65 and ov["tp_pct"] == 1.6
    cfg = {"total_amount_quote": Decimal("400"),
           "triple_barrier_config": TripleBarrierConfig(take_profit=Decimal("0.01"), stop_loss=Decimal("0.005"))}
    changed = oa.apply_overrides_to_configs("grid", cfg, ov)
    tb = cfg["triple_barrier_config"]
    assert tb.stop_loss == Decimal("0.0065")   # 0.65% -> fraction
    assert tb.take_profit == Decimal("0.016")  # 1.6%  -> fraction
    assert changed["barriers"] == {"sl_pct": 0.65, "tp_pct": 1.6}


def test_mid_barrier_is_rail_only_no_triple_barrier():
    sig = Signal(bias=0.3, regime="range", entry_ok=True, scale=0.0,
                 spread_mult=1.0, confidence=0.5, sl_pct=0.5, tp_pct=1.0)
    ov = oa.compute_overrides("mid", sig)
    cfg = {"order_amount_quote": Decimal("500"), "directional_bias": 0.0}   # no triple_barrier
    changed = oa.apply_overrides_to_configs("mid", cfg, ov)
    assert "barriers" not in changed         # nothing to apply on the config
    assert ov["sl_pct"] == 0.5               # still surfaced for the session rail


def test_no_barriers_when_signal_has_none():
    ov = oa.compute_overrides("grid", Signal(bias=0.5, regime="trend_up", entry_ok=True, scale=0.3))
    assert "sl_pct" not in ov and "tp_pct" not in ov


def test_rail_barriers_sl_is_tighten_only():
    # Trend regime widened the signal SL past the user's stop — the rail must
    # stay at the user's number (the kill-switch contract).
    sig = Signal(bias=0.7, regime="trend_up", sl_pct=0.65, tp_pct=1.6)
    sl, tp = oa.rail_barriers(0.5, 1.0, sig)
    assert sl == 0.5
    assert tp == 1.6            # trend widens TP (let winners run)
    # Chop tightened the SL below the user's stop — the tighter value governs.
    sig = Signal(bias=0.0, regime="chop", sl_pct=0.4, tp_pct=0.8)
    sl, tp = oa.rail_barriers(0.5, 1.0, sig)
    assert sl == 0.4


def test_rail_barriers_tp_is_widen_only_never_fires_early():
    # OVERLAY-TP-NO-FLOOR: the chop regime scales the signal TP to 0.8x, which
    # used to LOWER the user's TP and fire the session rail ~20% early. The
    # user's TP is now a floor — the overlay may only widen it.
    chop = Signal(bias=0.0, regime="chop", sl_pct=0.4, tp_pct=0.8)
    _sl, tp = oa.rail_barriers(0.5, 1.0, chop)
    assert tp == 1.0           # floored at the user's TP, NOT lowered to 0.8
    # A trend still lets a winner run past the user's target.
    trend = Signal(bias=0.7, regime="trend_up", sl_pct=0.65, tp_pct=1.6)
    _sl, tp = oa.rail_barriers(0.5, 1.0, trend)
    assert tp == 1.6
    # Range leaves the user's TP exactly as configured.
    rng = Signal(bias=0.1, regime="range", sl_pct=0.5, tp_pct=1.0)
    _sl, tp = oa.rail_barriers(0.5, 1.0, rng)
    assert tp == 1.0


def test_rail_barriers_disarmed_stays_disarmed():
    sig = Signal(bias=0.5, regime="trend_up", sl_pct=0.65, tp_pct=1.6)
    sl, tp = oa.rail_barriers(0.0, 0.0, sig)
    assert sl is None and tp is None
    # One-sided arming is respected per side.
    sl, tp = oa.rail_barriers(0.5, 0.0, sig)
    assert sl == 0.5 and tp is None


def test_rail_barriers_none_when_signal_abstains():
    sl, tp = oa.rail_barriers(0.5, 1.0, Signal())     # cold signal: no barriers
    assert sl is None and tp is None


def test_stabilize_overrides_deadbands_wobble():
    prev = {"size_factor": 1.10, "spread_factor": 1.20, "directional_bias": 0.40,
            "suppress_new_entries": False, "regime": "trend_up",
            "sl_pct": 0.65, "tp_pct": 1.6}
    wobble = dict(prev, size_factor=1.12, spread_factor=1.25, directional_bias=0.45)
    out = oa.stabilize_overrides(prev, wobble)
    # Sub-threshold wobble reuses the previously applied factors verbatim, so
    # the live-config signature does not flap (no grid recenter / quote reset).
    assert out["size_factor"] == 1.10
    assert out["spread_factor"] == 1.20
    assert out["directional_bias"] == 0.40


def test_stabilize_overrides_material_changes_pass_through():
    prev = {"size_factor": 1.10, "spread_factor": 1.20, "suppress_new_entries": False,
            "regime": "trend_up", "sl_pct": 0.65, "tp_pct": 1.6}
    # A full step in size passes through untouched.
    big = dict(prev, size_factor=1.16)
    assert oa.stabilize_overrides(prev, big)["size_factor"] == 1.16
    # A regime flip always passes through (it changes the barriers).
    flip = dict(prev, regime="chop", suppress_new_entries=True, size_factor=1.11)
    assert oa.stabilize_overrides(prev, flip)["size_factor"] == 1.11
    # A suppression flip always passes through (risk control).
    supp = dict(prev, suppress_new_entries=True, size_factor=1.11)
    assert oa.stabilize_overrides(prev, supp)["size_factor"] == 1.11
    # No previous application: everything passes through.
    assert oa.stabilize_overrides(None, dict(prev))["size_factor"] == 1.10
