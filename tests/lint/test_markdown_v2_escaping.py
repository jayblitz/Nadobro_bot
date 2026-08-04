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

    CODE ENTITIES are skipped: inside ``code``/``pre`` only a backtick and a
    backslash are special, so a hex key or an address sitting in a code span is
    correct precisely BECAUSE it is unescaped. Treating those as errors would
    push callers toward escape_md inside code blocks, which is the bug this
    file exists to catch — literal backslashes in text the user copies.
    """
    errors: list[tuple[int, str, str]] = []
    i, n = 0, len(text)
    bold_open = False
    italic_open = False
    while i < n:
        ch = text[i]
        if ch == "\\":
            i += 2                      # escaped: consume the pair
            continue
        if text.startswith("```", i):   # pre block: skip to its closing fence
            end = text.find("```", i + 3)
            if end == -1:
                errors.append((i, "`", "unterminated ``` pre block"))
                break
            i = end + 3
            continue
        if ch == "`":                   # inline code: skip to its closing tick
            end = text.find("`", i + 1)
            if end == -1:
                errors.append((i, "`", "unterminated inline code span"))
                break
            i = end + 1
            continue
        if ch == "*":
            bold_open = not bold_open
            i += 1
            continue
        if ch == "_":                   # italic delimiter, same rule as bold
            italic_open = not italic_open
            i += 1
            continue
        if ch in SPECIAL:
            context = text[max(0, i - 30):i + 15].replace("\n", "\\n")
            errors.append((i, ch, context))
        i += 1
    if bold_open:
        errors.append((-1, "*", "unbalanced bold delimiter"))
    if italic_open:
        errors.append((-1, "_", "unbalanced italic delimiter"))
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
    assert markdown_v2_errors("_dangling")                           # dangling italic
    assert markdown_v2_errors("visit x?join=y")                      # bare = (the wallet bug)
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


# ==========================================================================
# Wallet cards — the highest-consequence text in the bot
# ==========================================================================
def test_wallet_cards_are_valid_markdown_v2():
    """The connect card carries the 1CT private key. If Telegram rejects the
    message it falls back to PLAIN TEXT, which strips the code block — and with
    it the copy affordance the user needs to complete setup."""
    from src.nadobro.handlers import formatters as f

    pk = "0x" + "ab" * 32
    cards = {
        "wallet_connect": f.fmt_wallet_connect_card(pk),
        "wallet_balance": f.fmt_wallet_balance_card(1234.5),
        "wallet_balance_error": f.fmt_wallet_balance_error(),
        "wallet_revoke_steps": f.fmt_wallet_revoke_steps_card(),
    }
    for name, text in cards.items():
        errors = markdown_v2_errors(text)
        assert not errors, f"{name}: {errors[:5]}"


def test_the_private_key_is_copyable_and_byte_exact():
    """Two properties the user's setup depends on: the key sits in a code
    entity (so Telegram offers copy), and what they copy is EXACTLY the key —
    no escaping artefacts. escape_md inside a code block would silently corrupt
    it, and a corrupted 1CT key fails on Nado with no useful error."""
    import re as _re

    from src.nadobro.handlers import formatters as f

    pk = "0x" + "cd" * 32
    card = f.fmt_wallet_connect_card(pk)
    block = _re.search(r"```\n(.*?)\n```", card, _re.S)
    assert block, "the key is not in a pre block — no copy button"
    assert block.group(1) == pk, f"copied text is not the key: {block.group(1)!r}"
    assert "\\" not in block.group(1), "escaping artefact inside the copy payload"


def test_escape_md_code_escapes_only_what_telegram_treats_as_special():
    from src.nadobro.handlers.formatters import escape_md, escape_md_code

    # Inside a code entity these are LITERAL and must not gain backslashes.
    for ch in "_*[]()~>#+-=|{}.!":
        assert escape_md_code(f"a{ch}b") == f"a{ch}b", ch
        assert escape_md(f"a{ch}b") != f"a{ch}b", f"escape_md should escape {ch}"
    # These two genuinely are special inside a code entity.
    assert escape_md_code("a`b") == "a\\`b"
    assert escape_md_code("a\\b") == "a\\\\b"
    assert escape_md_code(None) == ""
