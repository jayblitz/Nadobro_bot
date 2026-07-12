"""Security regression tests for HOWL auto-tuning parameter validation.

HOWL suggestions are produced by an LLM whose input includes attacker-
influenceable context (trade history, scraped market/social sentiment). The
applied values must therefore be allowlisted and clamped so a hallucinated or
prompt-injected suggestion can't weaken the Alpha Agent's risk rails.
"""
from __future__ import annotations

from src.nadobro.llm.howl_service import validate_howl_param as v


def test_rejects_unknown_parameter():
    ok, value, error = v("withdraw_to", "0xattacker")
    assert ok is False
    assert value is None
    assert "tunable" in error


def test_rejects_out_of_band_budget_and_leverage():
    assert v("budget_usd", 10_000_000)[0] is False
    assert v("budget_usd", 0)[0] is False
    assert v("leverage_cap", 1000)[0] is False
    assert v("leverage_cap", 0)[0] is False


def test_rejects_risk_rail_neutralization():
    # Zeroing confidence / inflating loss tolerance would let the agent fire on
    # any signal and ride positions to ruin.
    assert v("min_confidence", 0)[0] is False
    assert v("max_loss_pct", 99)[0] is False


def test_rejects_invalid_risk_level():
    ok, value, error = v("risk_level", "yolo")
    assert ok is False and value is None


def test_rejects_nan_and_inf():
    assert v("budget_usd", float("inf"))[0] is False
    assert v("budget_usd", float("nan"))[0] is False


def test_accepts_and_coerces_safe_values():
    ok, value, _ = v("leverage_cap", "5")
    assert ok and value == 5 and isinstance(value, int)

    ok, value, _ = v("risk_level", "Aggressive")
    assert ok and value == "aggressive"

    ok, value, _ = v("budget_usd", 750)
    assert ok and value == 750.0

    ok, value, _ = v("min_confidence", 0.7)
    assert ok and value == 0.7


def test_accepts_bool_coercion():
    ok, value, _ = v("use_sentiment", "false")
    assert ok and value is False
    ok, value, _ = v("howl_enabled", 1)
    assert ok and value is True
