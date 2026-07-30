"""Static guard: every whitelisted strategy field has a limits entry.

AUDIT-MM-2026-07-14 #1: ``mm_cross_after_seconds`` was added to
``allowed_numeric_fields`` without a ``limits`` entry — every Cross button
then crashed on ``limits[field]``. The handler now rejects such fields
defensively, but a silent reject is still a dead button, so this test pins
the invariant at the source level: the whitelist must be a subset of the
limits keys (and of the custom-input help so ✍️ paths stay reachable).
"""
import ast
import pathlib

SRC = pathlib.Path("src/nadobro/handlers/strategy_handler.py").read_text()
TREE = ast.parse(SRC)


def _string_elts(node) -> set[str]:
    return {e.value for e in getattr(node, "elts", []) if isinstance(e, ast.Constant)}


def _collect():
    allowed: set[str] = set()
    limit_keys: set[str] = set()
    input_allowed: set[str] = set()
    help_keys: set[str] = set()
    for node in ast.walk(TREE):
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            name = getattr(target, "id", "")
            if name == "allowed_numeric_fields" and isinstance(node.value, ast.Set):
                allowed = _string_elts(node.value)
            elif name == "limits" and isinstance(node.value, ast.Dict):
                limit_keys = {
                    k.value for k in node.value.keys
                    if isinstance(k, ast.Constant) and isinstance(k.value, str)
                }
            elif name == "allowed_inputs" and isinstance(node.value, ast.Tuple):
                input_allowed = _string_elts(node.value)
            elif name == "help_text" and isinstance(node.value, ast.Dict):
                help_keys = {
                    k.value for k in node.value.keys
                    if isinstance(k, ast.Constant) and isinstance(k.value, str)
                }
    return allowed, limit_keys, input_allowed, help_keys


def test_every_whitelisted_numeric_field_has_a_limits_entry():
    allowed, limit_keys, _, _ = _collect()
    assert allowed, "failed to locate allowed_numeric_fields in source"
    assert limit_keys, "failed to locate limits dict in source"
    missing = allowed - limit_keys
    assert not missing, (
        f"whitelisted fields with no limits entry (buttons would be dead): {sorted(missing)}"
    )


def test_every_custom_input_field_is_numeric_whitelisted():
    allowed, _, input_allowed, _ = _collect()
    assert input_allowed, "failed to locate allowed_inputs in source"
    # A ✍️ input for a field the set-path rejects can never be saved.
    orphans = input_allowed - allowed
    assert not orphans, f"custom-input fields missing from allowed_numeric_fields: {sorted(orphans)}"



# ── VOL-DIRECTION-TAB (reported 2026-07-28) ─────────────────────
# "When users click on Direction in Volume bot, it just shows the TP/SL
# interface rather." _strategy_config_section_kb's vol branch IGNORED the
# `section` argument and returned the margin/TP/SL/target keyboard for BOTH
# tabs, so the Direction *heading* rendered over the TP/SL *controls*. And
# vol_direction had no button anywhere: the value was validated
# ({"long","short"}) and consumed by bot_runtime, but the UI could never set it.

def _vol_kb(section):
    from src.nadobro.handlers.strategy_handler import _strategy_config_section_kb
    return _strategy_config_section_kb("vol", section)


def _callbacks(markup):
    return [b.callback_data for row in markup.inline_keyboard for b in row]


def test_vol_direction_tab_offers_long_and_short():
    cbs = _callbacks(_vol_kb("direction"))
    assert "strategy:set_text:vol:vol_direction:long" in cbs
    assert "strategy:set_text:vol:vol_direction:short" in cbs


def test_vol_direction_tab_is_not_the_tp_sl_keyboard():
    cbs = _callbacks(_vol_kb("direction"))
    leaked = [c for c in cbs if any(f in c for f in
              ("tp_pct", "sl_pct", "session_margin_usd", "target_volume_usd"))]
    assert not leaked, f"the Direction tab is still showing TP/SL controls: {leaked}"


def test_vol_risk_tab_still_has_tp_sl_margin_and_target():
    cbs = " ".join(_callbacks(_vol_kb("risk")))
    for field in ("tp_pct", "sl_pct", "session_margin_usd", "target_volume_usd"):
        assert field in cbs, f"{field} disappeared from the TP/SL tab"


def test_vol_direction_uses_set_text_not_set():
    """`set` rejects every vol field outside the numeric four, so a `set:`
    callback here would be silently dropped."""
    for cb in _callbacks(_vol_kb("direction")):
        if "vol_direction" in cb:
            assert cb.startswith("strategy:set_text:"), cb


def test_vol_direction_value_is_in_the_allowed_text_set():
    """The button value must survive the handler's allow-list."""
    import re
    from pathlib import Path
    src = Path("src/nadobro/handlers/strategy_handler.py").read_text()
    allowed = re.search(r'"vol_direction":\s*\{([^}]*)\}', src).group(1)
    for cb in _callbacks(_vol_kb("direction")):
        if "vol_direction" in cb:
            assert f'"{cb.rsplit(":", 1)[1]}"' in allowed
