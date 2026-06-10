"""CI lint: no blocking calls in coroutine bodies.

This codebase runs a single asyncio event loop per process; one blocking
call in a coroutine stalls every user's strategy ticks and Telegram taps.
It has happened twice in production (APScheduler job pileups from sync
Redis/token-bucket calls; home-card taps hung 30-60s on a sync
get_balance) — see git history around 8b73982 and 17df573. Both fixes
moved the blocking work into thread pools (``run_blocking`` /
``run_blocking_sdk`` / ``asyncio.to_thread``). This lint keeps it there.

Flagged when called *directly* in an ``async def`` body:
- ``time.sleep``                       (use ``asyncio.sleep``)
- ``requests.<verb>/request``          (use httpx async or a thread pool)
- ``socket.getaddrinfo/gethostbyname`` (resolver blocks)
- ``subprocess.run/call/check_*``      (use ``asyncio.create_subprocess_*``)
- the psycopg2 helpers from ``src.nadobro.db`` (``query_one`` /
  ``query_all`` / ``execute`` / ``execute_returning`` / ``query_count``),
  counted only in files that import them from ``src.nadobro.db``.

NOT flagged (deliberately): calls inside nested sync ``def``s or lambdas —
those are the standard pattern for work handed to ``asyncio.to_thread`` /
``run_in_executor``. A nested sync def invoked inline would slip through;
the trade-off keeps this lint zero-noise.

Escape hatch: tag the call line (or the line above) with
``# policy: blocking-ok(<reason>)``. The baseline is ZERO — the tree was
fully clean when this lint landed (2026-06-10). Keep it that way.
"""
from __future__ import annotations

import ast
import pathlib

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
SCAN_ROOTS = (
    REPO_ROOT / "src" / "nadobro",
    REPO_ROOT / "relay",
)

BLOCKING_OK_MARKER = "policy: blocking-ok"

BLOCKING_ATTR_CALLS = {
    ("time", "sleep"),
    ("requests", "get"),
    ("requests", "post"),
    ("requests", "put"),
    ("requests", "delete"),
    ("requests", "head"),
    ("requests", "patch"),
    ("requests", "request"),
    ("socket", "getaddrinfo"),
    ("socket", "gethostbyname"),
    ("subprocess", "run"),
    ("subprocess", "call"),
    ("subprocess", "check_call"),
    ("subprocess", "check_output"),
}

DB_HELPER_NAMES = {
    "query_one",
    "query_all",
    "execute",
    "execute_returning",
    "query_count",
}


def _db_helpers_imported(tree: ast.AST) -> set[str]:
    """Names from DB_HELPER_NAMES that this module imports from src.nadobro.db."""
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and node.module.endswith("nadobro.db"):
            for alias in node.names:
                if alias.name in DB_HELPER_NAMES:
                    imported.add(alias.asname or alias.name)
    return imported


class _CoroutineBlockingFinder(ast.NodeVisitor):
    def __init__(self, db_names: set[str]):
        self.db_names = db_names
        self.in_async = 0
        self.hits: list[tuple[int, str]] = []

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.in_async += 1
        for stmt in node.body:
            self.visit(stmt)
        self.in_async -= 1

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        # Sync def nested in a coroutine: typically thread-pool payload.
        return

    def visit_Lambda(self, node: ast.Lambda) -> None:
        return

    def visit_Call(self, node: ast.Call) -> None:
        if self.in_async:
            f = node.func
            if isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name):
                if (f.value.id, f.attr) in BLOCKING_ATTR_CALLS:
                    self.hits.append((node.lineno, f"{f.value.id}.{f.attr}"))
            elif isinstance(f, ast.Name) and f.id in self.db_names:
                self.hits.append((node.lineno, f"{f.id} (sync psycopg2 helper)"))
        self.generic_visit(node)


def _violations(py_file: pathlib.Path) -> list[tuple[int, str]]:
    source = py_file.read_text(encoding="utf-8")
    lines = source.splitlines()
    tree = ast.parse(source)
    finder = _CoroutineBlockingFinder(_db_helpers_imported(tree))
    finder.visit(tree)
    out = []
    for lineno, what in finder.hits:
        context = lines[max(lineno - 2, 0) : lineno]
        if any(BLOCKING_OK_MARKER in line for line in context):
            continue
        out.append((lineno, what))
    return out


def test_no_blocking_calls_in_coroutine_bodies():
    problems = []
    for root in SCAN_ROOTS:
        for py_file in sorted(root.rglob("*.py")):
            found = _violations(py_file)
            if found:
                rel = py_file.relative_to(REPO_ROOT)
                detail = ", ".join(f"line {ln}: {what}" for ln, what in found)
                problems.append(
                    f"{rel}: blocking call(s) in a coroutine body — {detail}.\n"
                    f"  Use asyncio.sleep / run_blocking / run_blocking_sdk / "
                    f"asyncio.to_thread, or tag with "
                    f"`# policy: blocking-ok(<reason>)` if provably non-blocking."
                )
    assert not problems, (
        "Blocking calls inside async functions stall the event loop for every "
        "user (two production incidents). Fix or tag:\n\n" + "\n\n".join(problems)
    )
