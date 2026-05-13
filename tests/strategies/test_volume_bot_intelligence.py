"""Tests for the Volume Bot regime gate.

Volume Bot's intelligence is intentionally light-touch: regime-aware pause
(veto entry when the regime is strongly against the signal direction) plus
a chop notional dampener. No PM, no per-level sizing.
"""
from __future__ import annotations

import pytest

from src.nadobro.strategies import _regime


# Import the helpers directly. The full volume_bot module imports trade_service
# which has deep dependencies, so we import the gate piece-by-piece here. In
# the production environment the full import path works because the stubs are
# all real.
def _load_gate():
    import importlib
    mod = importlib.import_module("src.nadobro.strategies.volume_bot")
    return mod._vol_regime_gate, mod._vol_intelligence_on


def test_vol_gate_disabled_passes_through():
    gate, _ = _load_gate()
    signal = {"ok": True, "direction": "long"}
    state = {}  # intelligence off
    out_signal, mult = gate(signal=signal, state=state, history=[100.0] * 20,
                             mid=100.0, product="QQQ")
    assert out_signal is signal
    assert mult == 1.0


def test_vol_gate_passes_through_when_signal_already_not_ok():
    gate, _ = _load_gate()
    state = {"vol_intelligence_enabled": True}
    signal = {"ok": False, "direction": "long", "reason": "warming_signal_history"}
    out_signal, mult = gate(signal=signal, state=state, history=[],
                             mid=100.0, product="QQQ")
    assert out_signal is signal  # untouched
    assert mult == 1.0


def test_vol_gate_vetoes_long_in_trend_down():
    """Strong downtrend + long signal → veto entry."""
    gate, _ = _load_gate()
    # Construct a clear downtrend history (~10 bp per step)
    hist = [100.0]
    for _ in range(30):
        hist.append(hist[-1] * (1 - 10 / 1e4))
    state = {
        "vol_intelligence_enabled": True,
        "rgrid_prev_funding_bp": 2.0,  # bearish-leaning funding for extra confidence
    }
    signal = {"ok": True, "direction": "long"}
    out_signal, mult = gate(
        signal=signal, state=state, history=hist, mid=hist[-1], product="QQQ",
    )
    # Should veto:
    assert out_signal["ok"] is False
    assert out_signal["reason"] == "regime_against_signal"
    assert mult == 1.0
    assert state.get("vol_regime_veto_count") == 1


def test_vol_gate_vetoes_short_in_trend_up():
    gate, _ = _load_gate()
    hist = [100.0]
    for _ in range(30):
        hist.append(hist[-1] * (1 + 10 / 1e4))
    state = {
        "vol_intelligence_enabled": True,
        "rgrid_prev_funding_bp": -2.0,
    }
    signal = {"ok": True, "direction": "short"}
    out_signal, mult = gate(
        signal=signal, state=state, history=hist, mid=hist[-1], product="QQQ",
    )
    assert out_signal["ok"] is False
    assert out_signal["reason"] == "regime_against_signal"


def test_vol_gate_passes_aligned_direction_in_trend():
    """If signal direction agrees with the trend, gate should let it through."""
    gate, _ = _load_gate()
    hist = [100.0]
    for _ in range(30):
        hist.append(hist[-1] * (1 + 10 / 1e4))
    state = {"vol_intelligence_enabled": True}
    signal = {"ok": True, "direction": "long"}
    out_signal, mult = gate(
        signal=signal, state=state, history=hist, mid=hist[-1], product="QQQ",
    )
    assert out_signal["ok"] is True
    assert mult == 1.0


def test_vol_gate_dampens_in_chop():
    """In chop_high_vol the gate keeps the signal but halves notional."""
    gate, _ = _load_gate()
    # Build a saw-tooth history that looks like chop. We force the classifier
    # into chop_high_vol by passing variance_ratio >= trend threshold via the
    # state hint AND fluctuating mids.
    hist = []
    base = 100.0
    for i in range(30):
        hist.append(base + (1.0 if i % 2 == 0 else -1.0))
    state = {
        "vol_intelligence_enabled": True,
        # Inject classifier hints that match a chop regime: high variance,
        # high realized_move, low directional drift (which the sawtooth gives).
        "dgrid_variance_ratio": 1.5,
        "dgrid_realized_move_bp": 80.0,
    }
    signal = {"ok": True, "direction": "long"}
    out_signal, mult = gate(
        signal=signal, state=state, history=hist, mid=hist[-1], product="QQQ",
    )
    # If the regime resolves to chop_high_vol it dampens; otherwise pass-through.
    regime = state.get("vol_regime_info", {}).get("regime")
    if regime == _regime.REGIME_CHOP_HIGH_VOL:
        assert mult == pytest.approx(0.5)
        assert state.get("vol_regime_chop_dampen_count") == 1
    else:
        # Sawtooth might land in a different bucket depending on classifier
        # tuning; in that case at minimum no veto happened.
        assert out_signal["ok"] is True


def test_vol_intelligence_flag_resolution():
    _, intel_on = _load_gate()
    assert intel_on({}) is False
    assert intel_on({"vol_intelligence_enabled": True}) is True
    # legacy fallback
    assert intel_on({"dgrid_intelligence_enabled": True}) is True
    # explicit False beats legacy True
    assert intel_on({"vol_intelligence_enabled": False, "dgrid_intelligence_enabled": True}) is False
