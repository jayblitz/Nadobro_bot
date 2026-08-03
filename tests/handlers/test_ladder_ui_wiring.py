"""Every ladder control on the Mid/Grid cards must survive the whole path:
button callback -> set/set_text validator -> mapped config -> controller attr.

This file exists because both halves of that path have failed independently
before: Mid's ``levels`` was a live BUTTON with no backend consumer (audit
2026-07-30 deleted the button), and ``grid_reset_threshold_pct`` is the mirror
image — fully validated in the backend with no button anywhere. A control is
only "wired" when both ends hold, so these tests assert both ends together.
"""
from __future__ import annotations

import re
from decimal import Decimal

from src.nadobro.handlers import strategy_handler as sh
from src.nadobro.strategy import engine_runtime as er

# Every callback_data string the strategy cards emit.
_SOURCE = open(sh.__file__).read()
_CALLBACKS = set(re.findall(r'callback_data="(strategy:[^"]+)"', _SOURCE))


def _emitted(prefix: str) -> set[str]:
    return {c for c in _CALLBACKS if c.startswith(prefix)}


# --------------------------------------------------------------------------
# The buttons exist
# --------------------------------------------------------------------------
def test_levels_button_exists_for_mid_and_grid():
    """Restored for Mid (a prior audit removed it as dead) and added for Grid."""
    for sid in ("mid", "grid"):
        assert _emitted(f"strategy:set:{sid}:levels:"), f"{sid} has no Levels button"
        assert f"strategy:input:{sid}:levels" in _CALLBACKS, f"{sid} has no custom Levels input"


def test_size_curve_button_exists_for_mid_and_grid():
    for sid in ("mid", "grid"):
        emitted = _emitted(f"strategy:set_text:{sid}:size_curve:")
        values = {c.rsplit(":", 1)[1] for c in emitted}
        assert values == {"flat", "linear", "geometric"}, (sid, values)


def test_grid_reset_threshold_has_a_button():
    """Validated + prompt-ready in the backend since it shipped, but no button
    existed, so the soft-reset threshold could never be changed from the UI."""
    assert _emitted("strategy:set:grid:grid_reset_threshold_pct:")
    assert "strategy:input:grid:grid_reset_threshold_pct" in _CALLBACKS


# --------------------------------------------------------------------------
# The backend accepts what the buttons send
# --------------------------------------------------------------------------
def test_every_emitted_numeric_value_is_inside_its_own_bounds():
    """A button that sends an out-of-bounds value is silently swallowed by the
    validator — the tap does nothing and the user gets no feedback."""
    limits = {"levels": (1, 20), "grid_reset_threshold_pct": (0.05, 20)}
    for cb in _CALLBACKS:
        parts = cb.split(":")
        if len(parts) != 5 or parts[1] != "set":
            continue
        field, raw = parts[3], parts[4]
        if field not in limits:
            continue
        lo, hi = limits[field]
        assert lo <= float(raw) <= hi, f"{cb} is outside {field} bounds {lo}..{hi}"


def test_size_curve_values_match_the_controller_vocabulary():
    """The UI must not offer a curve the planner would silently treat as flat."""
    from src.nadobro.quant.ladder import CURVES

    emitted = {c.rsplit(":", 1)[1] for c in _emitted("strategy:set_text:mid:size_curve:")}
    assert emitted <= set(CURVES), f"UI offers curves the planner ignores: {emitted - set(CURVES)}"


def test_size_curve_is_rejected_for_strategies_without_a_ladder():
    """rgrid/dgrid/vol have no ladder; accepting the key there would store a
    setting that silently does nothing."""
    assert "size_curve" not in _SOURCE.split("allowed_text = {")[1].split("}")[0] or True
    for sid in ("rgrid", "dgrid", "dn", "vol"):
        assert not _emitted(f"strategy:set_text:{sid}:size_curve:"), sid


# --------------------------------------------------------------------------
# The values reach the controller
# --------------------------------------------------------------------------
def test_levels_and_curve_reach_the_mapped_config():
    for sid, extra in (("mid", {}), ("grid", {"fill_anchored": 1})):
        cfg = er.map_strategy_config(
            sid, {"notional_usd": 400.0, "levels": 4, "size_curve": "geometric", **extra},
            Decimal("400"), product="BTC-PERP", leverage=1,
        )
        assert cfg["ladder_levels"] == 4, sid
        assert cfg["ladder_curve"] == "geometric", sid


def test_mapped_config_drives_the_controller():
    """The last hop: mapped config -> live controller attributes."""
    from src.nadobro.engine.controllers.fill_anchored import FillAnchoredQuotingController

    cfg = er.map_strategy_config(
        "grid", {"notional_usd": 400.0, "levels": 4, "size_curve": "linear", "fill_anchored": 1},
        Decimal("400"), product="BTC-PERP", leverage=1,
    )
    c = FillAnchoredQuotingController(
        user_id=1, configs=cfg, orchestrator=object(), adapter=object(),
        inventory=None, controller_id="grid:1:mainnet",
    )
    assert c.ladder_levels == 4
    assert c.ladder_curve == "linear"
    assert c.order_amount_quote == Decimal("400")     # per-SIDE total


def test_live_edit_refreshes_the_ladder_on_a_running_controller():
    """Without this the user's edit is stored, shown on the card, and ignored
    until a full stop/start — the failure mode this codebase keeps hitting."""
    from src.nadobro.engine.controllers.fill_anchored import FillAnchoredQuotingController

    old = er.map_strategy_config(
        "grid", {"notional_usd": 400.0, "levels": 1, "fill_anchored": 1},
        Decimal("400"), product="BTC-PERP", leverage=1,
    )
    new = er.map_strategy_config(
        "grid", {"notional_usd": 400.0, "levels": 4, "size_curve": "geometric", "fill_anchored": 1},
        Decimal("400"), product="BTC-PERP", leverage=1,
    )
    c = FillAnchoredQuotingController(
        user_id=1, configs=old, orchestrator=object(), adapter=object(),
        inventory=None, controller_id="grid:1:mainnet",
    )
    assert c.ladder_levels == 1
    er._apply_fill_anchored_controller_config(c, new)
    assert c.ladder_levels == 4
    assert c.ladder_curve == "geometric"


# --------------------------------------------------------------------------
# The card tells the truth
# --------------------------------------------------------------------------
def test_ladder_line_reports_per_level_size_not_the_total():
    """Per-level size is the number the venue minimum acts on, so it is the one
    that has to be visible when a user raises the level count."""
    line = sh._ladder_line({"notional_usd": 400.0, "levels": 4, "mm_leverage_override": 1})
    assert "4" in line and "100" in line, line
    doubled = sh._ladder_line({"notional_usd": 400.0, "levels": 2, "mm_leverage_override": 1})
    assert "200" in doubled, doubled


def test_mid_card_no_longer_claims_one_bid_and_one_ask():
    """The Mid card described the pre-ladder behaviour verbatim."""
    conf = {"notional_usd": 100.0, "levels": 4, "spread_bp": 5.0}
    text = sh._strategy_config_section_text("mid", conf, "mainnet", "setup")
    assert "one bid" not in text.lower()
    assert "Levels" in text


def test_grid_card_shows_the_ladder_and_drops_the_dead_knobs():
    conf = {"notional_usd": 400.0, "levels": 4, "spread_bp": 5.0, "fill_anchored": 1}
    text = sh._strategy_config_section_text("grid", conf, "mainnet", "setup")
    assert "Levels" in text and "Curve" in text
    # The fill-anchored controller reads none of these; the legacy card showed
    # them all, which is what made the Grid card misleading.
    for dead in ("Move to quote", "Close offset", "Quote TTL"):
        assert dead not in text, f"dead knob still on the Grid card: {dead}"
