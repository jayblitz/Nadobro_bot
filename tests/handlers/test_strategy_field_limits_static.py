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

