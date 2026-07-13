"""Static guard: env vars must be read via utils/env.py helpers, not raw parses.

Deploy dashboards / .env templates in this project carry inline ``# comments``.
A raw ``int(os.environ.get(...))`` at module level turns a commented value into
a boot-time ValueError (and a manual truthy-parse silently reads False). This
sweep keeps the comment-tolerant helpers (env_int/env_float/env_bool/env_str,
clean_env_value) the only way env values get parsed.

AST-based on purpose: the earlier regex version was evaded by ``import os as
_os`` aliases, casts inside comprehensions, and ``float((os.environ.get(k) or
"").strip() or d)`` wrappers — all real instances found in review.
"""

import ast
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SCAN_ROOTS = [REPO / "src" / "nadobro", REPO / "main.py"]
EXEMPT = {
    # the helpers themselves
    REPO / "src" / "nadobro" / "utils" / "env.py",
}
# Deliberate strict truthy parse: the redeploy auto-resume escape hatch must be
# FAIL-CLOSED, so it intentionally does NOT use comment-tolerant env_bool (a
# commented "true # note" has been inert historically and must stay inert).
# See tests/trading/test_desk_resume_fail_closed.py.
TRUTHY_EXEMPT = {
    REPO / "src" / "nadobro" / "trading" / "desk_runtime.py",
}

NUMERIC_CASTS = {"int", "float", "Decimal"}
TRUTHY_LITERALS = {"1", "true", "yes", "on"}


def _iter_files():
    for root in SCAN_ROOTS:
        if root.is_file():
            yield root
        else:
            yield from root.rglob("*.py")


def _os_aliases(tree: ast.Module) -> tuple[set[str], set[str]]:
    """Names bound to the os module, and names bound to os.environ/os.getenv."""
    mod_aliases: set[str] = set()
    fn_aliases: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                if a.name == "os":
                    mod_aliases.add(a.asname or "os")
        elif isinstance(node, ast.ImportFrom) and node.module == "os":
            for a in node.names:
                if a.name in ("environ", "getenv"):
                    fn_aliases.add(a.asname or a.name)
    return mod_aliases, fn_aliases


def _reads_environ(node: ast.AST, mod_aliases: set[str], fn_aliases: set[str]) -> bool:
    """True if any node in the subtree reads the process environment."""
    for n in ast.walk(node):
        # <os_alias>.environ / <os_alias>.getenv
        if isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name):
            if n.value.id in mod_aliases and n.attr in ("environ", "getenv"):
                return True
        # bare environ[...] / getenv(...) via `from os import ...`
        if isinstance(n, ast.Name) and n.id in fn_aliases:
            return True
    return False


def _scan(path: Path) -> tuple[list[str], list[str]]:
    text = path.read_text(encoding="utf-8")
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return [], []
    mod_aliases, fn_aliases = _os_aliases(tree)
    if not mod_aliases and not fn_aliases:
        return [], []

    casts: list[str] = []
    truthy: list[str] = []
    rel = str(path.relative_to(REPO))
    for node in ast.walk(tree):
        # int/float/Decimal(<subtree that reads environ>)
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in NUMERIC_CASTS
            and any(_reads_environ(a, mod_aliases, fn_aliases) for a in node.args)
        ):
            casts.append(f"{rel}:{node.lineno}")
        # <environ read ...> in ("1", "true", ...)
        if isinstance(node, ast.Compare) and any(
            isinstance(op, ast.In) for op in node.ops
        ):
            right = node.comparators[-1]
            if (
                isinstance(right, (ast.Tuple, ast.List, ast.Set))
                and any(
                    isinstance(e, ast.Constant) and e.value in TRUTHY_LITERALS
                    for e in right.elts
                )
                and _reads_environ(node.left, mod_aliases, fn_aliases)
            ):
                truthy.append(f"{rel}:{node.lineno}")
    return casts, truthy


def test_no_raw_numeric_env_casts():
    offenders = []
    for path in _iter_files():
        if path in EXEMPT:
            continue
        casts, _ = _scan(path)
        offenders.extend(casts)
    assert not offenders, (
        "raw numeric cast of an env value (breaks on inline '# comment'); "
        "use env_int/env_float from src/nadobro/utils/env.py:\n  "
        + "\n  ".join(sorted(offenders))
    )


def test_no_manual_truthy_env_parses():
    offenders = []
    for path in _iter_files():
        if path in EXEMPT or path in TRUTHY_EXEMPT:
            continue
        _, truthy = _scan(path)
        offenders.extend(truthy)
    assert not offenders, (
        "manual truthy parse of an env value; use env_bool from "
        "src/nadobro/utils/env.py (or add a justified TRUTHY_EXEMPT entry "
        "for a deliberately fail-closed flag):\n  " + "\n  ".join(sorted(offenders))
    )
