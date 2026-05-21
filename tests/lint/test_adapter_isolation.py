"""CI lint: only src/nadobro/engine/adapter/nado.py may import from
src/nadobro/connectors/nado/. This guarantees the 1CT Linked Signer path
has a single source of truth.
"""
from __future__ import annotations

import ast
import pathlib
import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src" / "nadobro"
ALLOWED_IMPORTER = SRC_ROOT / "engine" / "adapter" / "nado.py"
FORBIDDEN_PREFIXES = (
    "nadobro.connectors.nado",
    "src.nadobro.connectors.nado",
    "connectors.nado",
)


def _is_forbidden(name: str) -> bool:
    return any(name == p or name.startswith(p + ".") for p in FORBIDDEN_PREFIXES)


def _imports_connectors_nado(py_file: pathlib.Path) -> bool:
    try:
        tree = ast.parse(py_file.read_text(encoding="utf-8"))
    except SyntaxError:
        return False
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if _is_forbidden(alias.name):
                    return True
        elif isinstance(node, ast.ImportFrom):
            if node.module and _is_forbidden(node.module):
                return True
    return False


def test_only_adapter_may_import_connectors_nado() -> None:
    violations: list[str] = []
    for py in SRC_ROOT.rglob("*.py"):
        if py.resolve() == ALLOWED_IMPORTER.resolve():
            continue
        if _imports_connectors_nado(py):
            violations.append(str(py.relative_to(REPO_ROOT)))
    assert not violations, (
        "Forbidden imports of connectors/nado outside engine/adapter/nado.py:\n  "
        + "\n  ".join(violations)
    )
