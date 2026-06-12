"""CI lint: every translation keeps the English key's {placeholders}.

Texts flow ``localize_text(key) -> .format(**kwargs)`` — a translation
that drops or renames a placeholder makes ``.format`` raise. ``_edit_loc``
falls back to the unformatted English template, but ``_notify`` (strategy
stop / regime-gate / DN execution alerts) formats unguarded: the message
is silently dropped for that language. 2,525 entries are clean today;
this keeps it that way as translators add languages.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro import i18n

_PLACEHOLDER = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")


def test_translations_preserve_format_placeholders():
    problems = []
    checked = 0
    for table_name in ("_TEXTS", "_LABELS"):
        table = getattr(i18n, table_name, {}) or {}
        for key, langs in table.items():
            if not isinstance(key, str) or not isinstance(langs, dict):
                continue
            want = set(_PLACEHOLDER.findall(key))
            for lang, txt in langs.items():
                if not isinstance(txt, str):
                    continue
                checked += 1
                got = set(_PLACEHOLDER.findall(txt))
                if got != want:
                    problems.append(
                        f"{table_name}[{lang}] {key[:60]!r}: "
                        f"missing={sorted(want - got)} extra={sorted(got - want)}"
                    )
    assert checked > 1000, f"i18n tables look empty (checked={checked}) — lint misconfigured?"
    assert not problems, "\n  ".join(
        ["translation placeholder mismatches (drops _notify messages):"] + problems
    )
