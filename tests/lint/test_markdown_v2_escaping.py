"""CI lint: MarkdownV2 card text must be correctly ESCAPED, not just correctly
declared.

``test_parse_mode_consistency`` already proves a message's markup matches the
``parse_mode`` it is sent with. It does not prove the text is *parseable*.
Telegram MarkdownV2 requires ``_ * [ ] ( ) ~ ` > # + - = | { } . !`` to be
backslash-escaped anywhere they are not deliberate markup; one stray character
makes Telegram reject the whole message with "Can't parse entities". There IS a
plain-text fallback in ``_edit_loc``, so the failure is not a dead card — it is
worse in a quieter way: the user silently gets an unformatted card and nothing
in the logs says the text was wrong.

Found by exactly this check: the Delta Neutral safety card had shipped an
unescaped hyphen in "Auto-close on maintenance" (three call sites, present on
main), so that card had been degrading to plain text.

The strategy cards are rendered for real here rather than parsed as source,
because the escaping bug that matters is in the *interpolated result*, not in
any single literal.
"""
from __future__ import annotations

import pytest

from src.nadobro.handlers import strategy_handler as sh

# Telegram MarkdownV2 reserved characters.
SPECIAL = set(r"_*[]()~`>#+-=|{}.!")


def markdown_v2_errors(text: str) -> list[tuple[int, str, str]]:
    """Unescaped reserved characters Telegram would reject.

    ``*`` is permitted as a bold delimiter but must be balanced — an odd count
    leaves an unterminated entity, which Telegram also rejects.
    """
    errors: list[tuple[int, str, str]] = []
    i = 0
    bold_open = False
    while i < len(text):
        ch = text[i]
        if ch == "\\":
            i += 2                      # escaped: consume the pair
            continue
        if ch == "*":
            bold_open = not bold_open
            i += 1
            continue
        if ch in SPECIAL:
            context = text[max(0, i - 30):i + 15].replace("\n", "\\n")
            errors.append((i, ch, context))
        i += 1
    if bold_open:
        errors.append((-1, "*", "unbalanced bold delimiter"))
    return errors


# One config per strategy, deliberately chosen to exercise the interpolated
# branches: a curve that makes rungs unequal, a bias that renders a signed
# number, thresholds that trip the "inactive" and "capped" warnings.
_CONFS = {
    "grid": {
        "notional_usd": 400.0, "levels": 4, "spread_bp": 5.0, "size_curve": "geometric",
        "fill_anchored": 1, "directional_bias": 0.5, "grid_reset_threshold_pct": 0.25,
        "min_spread_bp": 2.0, "max_spread_bp": 20.0, "product": "BTC-PERP",
    },
    "rgrid": {
        "notional_usd": 100.0, "levels": 4, "spread_bp": 10.0, "rgrid_spread_bp": 10.0,
        "rgrid_reset_threshold_pct": 0.1, "rgrid_discretion": 0.06, "fill_anchored": 1,
    },
    "mid": {
        "notional_usd": 100.0, "levels": 8, "spread_bp": 5.0, "size_curve": "linear",
        "directional_bias": -0.5, "product": "BTC-PERP",
    },
    "dgrid": {"notional_usd": 100.0, "levels": 4, "spread_bp": 8.0},
    "vol": {"notional_usd": 100.0, "session_margin_usd": 100.0, "tp_pct": 1.0, "sl_pct": 1.0},
    "dn": {"notional_usd": 100.0, "fixed_margin_usd": 100.0, "dn_max_drift_pct": 5.0},
}


def _cases():
    for sid, conf in _CONFS.items():
        for section, _label in sh._strategy_config_sections(sid):
            yield sid, section, conf


@pytest.mark.parametrize("sid,section,conf", list(_cases()), ids=lambda v: v if isinstance(v, str) else "")
def test_strategy_config_cards_are_valid_markdown_v2(sid, section, conf):
    text = sh._strategy_config_section_text(sid, conf, "mainnet", section)
    errors = markdown_v2_errors(text)
    assert not errors, (
        f"{sid}/{section}: {len(errors)} unescaped MarkdownV2 character(s)\n"
        + "\n".join(f"  {ch!r} at {idx}: …{ctx}…" for idx, ch, ctx in errors[:8])
    )


def test_legacy_fallback_card_is_valid_markdown_v2():
    """``_fmt_strategy_config_text`` is what any strategy without a sectioned
    card falls through to — it carried one of the three DN hyphens."""
    for sid, conf in _CONFS.items():
        text = sh._fmt_strategy_config_text(sid, conf, "mainnet")
        errors = markdown_v2_errors(text)
        assert not errors, f"legacy card {sid}: {errors[:5]}"


def test_the_validator_actually_catches_a_bad_string():
    """A validator that never fires is worse than none — pin that it fires."""
    assert markdown_v2_errors("Auto-close on maintenance")          # bare hyphen
    assert markdown_v2_errors("value: 1.5")                          # bare dot
    assert markdown_v2_errors("*unbalanced")                         # dangling bold
    assert not markdown_v2_errors(r"Auto\-close: *ON* at 1\.5%")     # correct


# ==========================================================================
# Validator parity (self-audit 2026-08-03)
# ==========================================================================
def _limit_table(source: str) -> dict:
    """Parse a ``limits = { "field": (lo, hi), ... }`` block."""
    import re as _re

    body = source.split("limits = {", 1)[1]
    out = {}
    for line in body.splitlines():
        m = _re.match(r'\s*"([a-z0-9_]+)":\s*\(([^)]+)\)\s*,', line)
        if m:
            try:
                lo, hi = (float(x.strip()) for x in m.group(2).split(","))
            except ValueError:
                continue
            out[m.group(1)] = (lo, hi)
        elif line.strip().startswith("}"):
            break
    return out


def test_button_and_typed_validators_agree_on_every_shared_field():
    """A strategy setting has TWO independent validators: the inline-button path
    in strategy_handler and the typed-reply path in messages.py. When they
    disagree, a value you can TAP is rejected when TYPED (or the reverse) — and
    both failures are silent, the input is simply dropped with no feedback.

    Found by this check: ``interval_seconds`` allowed 5 by button and 10 by
    typing, which defeated the button path's own documented reason for the
    floor ("so Turbo's 5s cadence can be re-entered manually").
    """
    from src.nadobro.handlers import messages as msg
    from src.nadobro.handlers import strategy_handler as sh_

    buttons = _limit_table(open(sh_.__file__).read())
    typed = _limit_table(open(msg.__file__).read())
    shared = set(buttons) & set(typed)
    assert len(shared) > 20, f"parser drifted — only {len(shared)} shared fields found"
    mismatched = {k: (buttons[k], typed[k]) for k in sorted(shared) if buttons[k] != typed[k]}
    assert not mismatched, (
        "validators disagree (button bounds vs typed bounds):\n"
        + "\n".join(f"  {k}: {v[0]} vs {v[1]}" for k, v in mismatched.items())
    )
