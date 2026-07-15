"""Option 1: per-strategy effective cadence (fast for rgrid/mid/vol)."""
from __future__ import annotations

import pytest

from _stubs import install_test_stubs  # noqa: F401

install_test_stubs()

from src.nadobro.core.cadence import effective_interval_seconds


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv("NADO_FAST_CADENCE_SECONDS", raising=False)
    monkeypatch.delenv("NADO_FAST_CADENCE_FLOOR_SECONDS", raising=False)
    yield


@pytest.mark.parametrize("strategy", ["rgrid", "mid", "vol", "RGRID", " Mid ", " VOL "])
def test_fast_strategies_capped_to_default_fast(strategy):
    # Default 60s configured -> capped to the 8s fast default.
    assert effective_interval_seconds(strategy, 60) == 8.0


@pytest.mark.parametrize("strategy", ["grid", "dgrid", "dn", "", None])
def test_other_strategies_unchanged(strategy):
    assert effective_interval_seconds(strategy, 60) == 60.0
    assert effective_interval_seconds(strategy, 5) == 5.0


def test_faster_configured_interval_is_honoured_for_fast_strategy():
    # min(configured, fast): a user who set 5s keeps 5s (still floored).
    assert effective_interval_seconds("rgrid", 5) == 5.0


def test_floor_is_respected(monkeypatch):
    monkeypatch.setenv("NADO_FAST_CADENCE_FLOOR_SECONDS", "3")
    monkeypatch.setenv("NADO_FAST_CADENCE_SECONDS", "8")
    # Configured 1s would beat the 8s cap, but the floor clamps it up to 3s.
    assert effective_interval_seconds("mid", 1) == 3.0


def test_env_overrides_fast_value(monkeypatch):
    monkeypatch.setenv("NADO_FAST_CADENCE_SECONDS", "15")
    assert effective_interval_seconds("rgrid", 60) == 15.0
    # non-fast strategy still ignores the fast knob
    assert effective_interval_seconds("grid", 60) == 60.0


def test_bad_interval_defaults_to_60_for_other_strategy():
    assert effective_interval_seconds("grid", None) == 60.0
    assert effective_interval_seconds("grid", "notanumber") == 60.0


# --- Universal floor (Turbo Volume, 2026-07) --------------------------------

def test_universal_floor_applies_to_non_fast_strategies(monkeypatch):
    """grid/dgrid now accept fast configured intervals (Turbo writes 5s), so
    the rate-limit floor must clamp EVERY strategy — a hand-typed 1s interval
    previously ran unfloored on the non-fast set."""
    monkeypatch.setenv("NADO_FAST_CADENCE_FLOOR_SECONDS", "3")
    assert effective_interval_seconds("grid", 1) == 3.0
    assert effective_interval_seconds("dgrid", 0.5) == 3.0
    assert effective_interval_seconds("dn", 1) == 3.0


def test_turbo_interval_five_seconds_passes_for_all_mm_strategies():
    for strategy in ("grid", "rgrid", "dgrid", "mid"):
        assert effective_interval_seconds(strategy, 5) == 5.0
