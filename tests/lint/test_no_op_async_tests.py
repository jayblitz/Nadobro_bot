"""Guard: an async test that never runs its body is a test that lies.

The repo-wide pattern for async tests is::

    def test_thing():
        async def body():
            ...asserts...

        asyncio.run(body())

Forgetting the final ``asyncio.run(body())`` leaves a test that defines a
coroutine, never awaits it, and passes unconditionally — green CI over code
nobody verified. Ten of these were found in one sweep (2026-07-31), including
the Volume Bot's session-loss-limit test, i.e. the check that bounds real
money. Python only emits a "coroutine was never awaited" RuntimeWarning here,
which pytest does not fail on by default.
"""
from __future__ import annotations

import ast
import pathlib

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
TESTS_ROOT = REPO_ROOT / "tests"

# Known offenders that do NOT merely need the runner added: these drive a real
# rate-limiter/scheduler and block for minutes when actually executed (verified
# 2026-07-31 — the suite times out). They need fake clocks before they can run,
# which is a separate piece of work. Documented here rather than silently
# tolerated; do not add to this list to make a new test pass.
KNOWN_BROKEN = {
    "tests/services/test_telegram_sender.py::test_send_is_rate_shaped",
    "tests/services/test_telegram_sender.py::test_priority_lane_jumps_queue",
    "tests/services/test_telegram_sender.py::test_dedupe_drops_repeats",
    "tests/services/test_telegram_sender.py::test_lane_cap_drops_oldest_info",
    "tests/services/test_telegram_sender.py::test_retry_after_reschedules",
}


def _no_op_async_tests() -> list[str]:
    offenders: list[str] = []
    for path in sorted(TESTS_ROOT.rglob("test_*.py")):
        if "__pycache__" in path.parts:
            continue
        source = path.read_text(encoding="utf-8")
        if "async def" not in source:
            continue
        try:
            tree = ast.parse(source)
        except SyntaxError:
            continue
        rel = str(path.relative_to(REPO_ROOT))
        for node in ast.walk(tree):
            if not (isinstance(node, ast.FunctionDef) and node.name.startswith("test_")):
                continue
            # A sync test that defines an inner coroutine must await/run it.
            inner = [
                child for child in ast.walk(node)
                if isinstance(child, ast.AsyncFunctionDef)
            ]
            if not inner:
                continue
            segment = ast.get_source_segment(source, node) or ""
            # The outer test is SYNC, so the only ways it can execute the inner
            # coroutine are an explicit runner. Do NOT accept a bare "await" as
            # proof: awaits inside the inner body say nothing about whether the
            # body is ever driven (this exact false negative hid the five
            # telegram_sender no-ops on the first pass).
            if "asyncio.run(" in segment or "run_until_complete(" in segment:
                continue
            offenders.append(f"{rel}::{node.name}")
    return offenders


def test_async_test_bodies_are_actually_executed():
    offenders = sorted(set(_no_op_async_tests()) - KNOWN_BROKEN)
    assert not offenders, (
        "these tests define an async body and never run it, so they pass "
        "without executing a single assertion — add `asyncio.run(body())`:\n  "
        + "\n  ".join(offenders)
    )


def test_known_broken_list_does_not_rot():
    """If a baselined test gets fixed, drop it from KNOWN_BROKEN so the guard
    keeps protecting it."""
    still_no_op = set(_no_op_async_tests())
    fixed = sorted(KNOWN_BROKEN - still_no_op)
    assert not fixed, (
        "these are no longer no-ops — remove them from KNOWN_BROKEN:\n  "
        + "\n  ".join(fixed)
    )
