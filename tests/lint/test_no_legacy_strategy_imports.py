"""CI guard: nothing may import from the deleted legacy ``src/nadobro/strategies``
package (removed in the Engine v2 Phase 4 cutover). Belt-and-braces against an
accidental restoration — strategy logic now lives under ``src/nadobro/engine``.
"""
from __future__ import annotations

import ast
import pathlib

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src" / "nadobro"
TESTS_ROOT = REPO_ROOT / "tests"
FORBIDDEN_PREFIXES = (
    "nadobro.strategies",
    "src.nadobro.strategies",
)


def _is_forbidden(name: str) -> bool:
    return any(name == p or name.startswith(p + ".") for p in FORBIDDEN_PREFIXES)


def _imports_legacy_strategies(py_file: pathlib.Path) -> bool:
    try:
        tree = ast.parse(py_file.read_text(encoding="utf-8"))
    except SyntaxError:
        return False
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            if any(_is_forbidden(a.name) for a in node.names):
                return True
        elif isinstance(node, ast.ImportFrom):
            if node.module and _is_forbidden(node.module):
                return True
    return False


def test_no_legacy_strategy_imports() -> None:
    violations: list[str] = []
    for root in (SRC_ROOT, TESTS_ROOT):
        for py in root.rglob("*.py"):
            if "__pycache__" in py.parts:
                continue
            if _imports_legacy_strategies(py):
                violations.append(str(py.relative_to(REPO_ROOT)))
    assert not violations, (
        "Legacy src/nadobro/strategies is deleted; these files still import it:\n  "
        + "\n  ".join(violations)
    )
