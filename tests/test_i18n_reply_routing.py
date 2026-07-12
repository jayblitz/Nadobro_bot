"""Reply-keyboard labels must survive the localize → tap → resolve round-trip.

Reply keyboards route by TEXT (messages.py: resolve_reply_button_text → REPLY_BUTTON_MAP),
unlike inline keyboards which route by callback_data. When two English labels share a
translation (e.g. ko renders both "⚙️ Settings" and "⚙️ Configure" as "⚙️ 설정"), the
reverse map can only return one sibling — if the winner is not in REPLY_BUTTON_MAP the
tap silently falls through to the free-text/AI-chat path. That exact failure shipped
for ko Settings and ru Back. These tests pin the invariant for every label in every
supported language so a new translation can never silently break routing again.
"""

from src.nadobro.handlers.keyboards import REPLY_BUTTON_MAP
from src.nadobro.i18n import (
    SUPPORTED_LANGS,
    localize_label,
    resolve_reply_button_text,
)


def test_reply_buttons_route_in_every_language():
    """localize(label) must resolve back to a label mapping to the SAME action."""
    failures = []
    for label, action in REPLY_BUTTON_MAP.items():
        for lang in sorted(SUPPORTED_LANGS - {"en"}):
            shown = localize_label(label, lang)
            resolved = resolve_reply_button_text(shown)
            routed = REPLY_BUTTON_MAP.get(resolved)
            if routed != action:
                failures.append(
                    f"[{lang}] {label!r} shown as {shown!r} resolves to {resolved!r} "
                    f"-> routes to {routed!r} (expected {action!r})"
                )
    assert not failures, "reply-keyboard routing broken:\n" + "\n".join(failures)


def test_reverse_map_collisions_are_aliased():
    """Every translation collision between REPLY-routable labels must land on the
    same action. Collisions with purely-inline labels are fine only when the
    colliding sibling is aliased in REPLY_BUTTON_MAP (or routes identically)."""
    from src.nadobro.i18n import _LABELS  # test-only introspection

    by_translation: dict[str, set[str]] = {}
    for en_label, translations in _LABELS.items():
        for translated in translations.values():
            if translated != en_label:
                by_translation.setdefault(translated, set()).add(en_label)

    failures = []
    for translated, siblings in by_translation.items():
        if len(siblings) < 2:
            continue
        # Only collisions involving at least one reply-routable label matter.
        routable = {s for s in siblings if s in REPLY_BUTTON_MAP}
        if not routable:
            continue
        actions = {REPLY_BUTTON_MAP.get(s) for s in siblings}
        if len(actions) != 1 or None in actions:
            failures.append(f"{translated!r} <- {sorted(siblings)} routes to {actions}")
    assert not failures, (
        "translation collisions with divergent routing (alias the losing sibling "
        "in REPLY_BUTTON_MAP):\n" + "\n".join(failures)
    )
