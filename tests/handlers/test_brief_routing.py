"""Verify the brief-intent regex sits AFTER the studio gate in messages.py.

We don't run the full handle_message_inner here (it pulls a lot of state);
instead we read the source and assert the ordering invariant the audit commit
3e5f4f8 set up: studio gate must run before brief intent must run before
classify_conversation_intent.
"""

from pathlib import Path


_MESSAGES_PY = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "nadobro"
    / "handlers"
    / "messages.py"
)


def _line_index(haystack: str, needle: str) -> int:
    idx = haystack.find(needle)
    assert idx >= 0, f"missing marker: {needle!r}"
    return idx


def test_brief_intent_runs_after_studio_gate_before_intent_classifier():
    src = _MESSAGES_PY.read_text(encoding="utf-8")
    studio_idx = _line_index(src, "if await handle_studio_text(update, context):")
    brief_idx = _line_index(src, "if is_brief_request(text):")
    intent_idx = _line_index(src, "classify_conversation_intent(text)")

    assert studio_idx < brief_idx < intent_idx, (
        "Order must be: studio gate -> brief intent -> intent classifier"
    )
