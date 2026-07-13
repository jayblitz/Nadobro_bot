"""Reply-keyboard labels must survive the localize → tap → resolve round-trip.

Reply keyboards route by TEXT (messages.py: resolve_reply_button_text →
REPLY_BUTTON_MAP), unlike inline keyboards which route by callback_data. When
two English labels share a translation (e.g. ko renders both "⚙️ Settings" and
"⚙️ Configure" as "⚙️ 설정"), the reverse map keeps ALL siblings and routing
passes prefer=REPLY_BUTTON_MAP.__contains__ so the routable sibling wins.
That exact collision shipped broken for ko Settings and ru Back once — these
tests pin the invariant for every label in every supported language so a new
translation can never silently break routing again.
"""

from src.nadobro.handlers.keyboards import REPLY_BUTTON_MAP
from src.nadobro.i18n import (
    SUPPORTED_LANGS,
    localize_label,
    resolve_reply_button_text,
)


def _route(shown_text: str) -> str | None:
    """Mirror messages.py routing: resolve with the routable-sibling preference."""
    resolved = resolve_reply_button_text(shown_text, prefer=REPLY_BUTTON_MAP.__contains__)
    return REPLY_BUTTON_MAP.get(resolved)


def test_reply_buttons_route_in_every_language():
    """localize(label) must route back to the SAME action via the real path."""
    failures = []
    for label, action in REPLY_BUTTON_MAP.items():
        for lang in sorted(SUPPORTED_LANGS - {"en"}):
            shown = localize_label(label, lang)
            routed = _route(shown)
            if routed != action:
                failures.append(
                    f"[{lang}] {label!r} shown as {shown!r} routes to {routed!r} "
                    f"(expected {action!r})"
                )
    assert not failures, "reply-keyboard routing broken:\n" + "\n".join(failures)


def test_known_collision_losers_route_correctly():
    """The two historical failures stay fixed: ko Settings and ru trade-flow Back."""
    assert _route(localize_label("⚙️ Settings", "ko")) == "settings:view"
    assert _route(localize_label("◀ Back", "ru")) == "trade_flow:back"


def test_no_ambiguous_routable_collisions():
    """Two REPLY-routable siblings sharing a translation must share an action.

    Non-routable siblings are harmless (the prefer predicate skips them), but
    if two labels that BOTH route collide on a translation with different
    actions, the tap is genuinely ambiguous — that needs a translation change,
    not a routing hack.
    """
    from src.nadobro.i18n import _LABELS  # test-only introspection

    by_translation: dict[str, set[str]] = {}
    for en_label, translations in _LABELS.items():
        for translated in translations.values():
            if translated != en_label:
                by_translation.setdefault(translated, set()).add(en_label)

    failures = []
    for translated, siblings in by_translation.items():
        routable = {s for s in siblings if s in REPLY_BUTTON_MAP}
        if len(routable) < 2:
            continue
        actions = {REPLY_BUTTON_MAP[s] for s in routable}
        if len(actions) != 1:
            failures.append(f"{translated!r} <- {sorted(routable)} routes to {sorted(actions)}")
    assert not failures, (
        "ambiguous reply-routing collisions (fix the translation):\n" + "\n".join(failures)
    )
