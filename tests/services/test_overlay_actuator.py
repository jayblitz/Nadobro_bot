"""Overlay actuator — bounded signal→config mapping and the drawdown kill-switch."""
from __future__ import annotations

from decimal import Decimal

import pytest

from src.nadobro.services import overlay_actuator as oa
from src.nadobro.services.signal_engine import Signal


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
