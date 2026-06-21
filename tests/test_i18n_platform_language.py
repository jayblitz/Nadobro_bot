"""Regression tests for platform-wide language support.

Covers the i18n fixes shipped in fix/platform-wide-i18n:
  * BCP-47 / device-locale normalization (ko-KR -> ko, zh-Hans -> zh, ...);
  * catalog coverage for the primary navigation labels across every
    supported language (guards against the "only a few words translate"
    regression where menu labels were missing from the catalog);
  * catalog-miss telemetry so coverage gaps are measurable;
  * the optional auto-translator fallback hook (off by default).
"""
import importlib

import pytest

i18n = importlib.import_module("src.nadobro.i18n")

NON_EN_LANGS = sorted(lang for lang in i18n.SUPPORTED_LANGS if lang != "en")


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("ko", "ko"),
        ("ko-KR", "ko"),
        ("zh-Hans", "zh"),
        ("zh_CN", "zh"),
        ("en-US", "en"),
        ("EN", "en"),
        ("xx", "en"),
        (None, "en"),
        ("", "en"),
    ],
)
def test_normalize_lang(raw, expected):
    assert i18n.normalize_lang(raw) == expected


# Primary navigation labels that must be fully localized. These are the labels
# the user sees on /start and the home card -- exactly the surface that was
# rendering half English / half translated before the fix.
CORE_NAV_LABELS = [
    "🏠 Home",
    "🤖 Trade Console",
    "📁 Portfolio Deck",
    "💼 Wallet Vault",
    "🧠 Strategy Lab",
    "🌐 Execution Mode",
    "🏆 Nado Points",
    "💬 Ask Nadobro",
    "💰 Nado Vault",
    "🔔 Alerts",
    "🎁 Referrals",
    "⚙️ Settings",
    "📚 Resources",
]


@pytest.mark.parametrize("label", CORE_NAV_LABELS)
@pytest.mark.parametrize("lang", NON_EN_LANGS)
def test_core_nav_labels_localized(label, lang):
    out = i18n.localize_label(label, lang)
    assert out and out != label, f"{label!r} is not localized for {lang!r}"


def test_persistent_menu_labels_registered():
    """Every label emitted by the persistent reply keyboard must translate."""
    kb = pytest.importorskip("src.nadobro.handlers.keyboards")
    labels = [
        kb.HOME_BTN_TRADE,
        kb.HOME_BTN_PORTFOLIO,
        kb.HOME_BTN_WALLET,
        kb.HOME_BTN_POINTS,
        kb.HOME_BTN_REFER,
        kb.HOME_BTN_STRATEGIES,
        kb.HOME_BTN_ALERTS,
        kb.HOME_BTN_SETTINGS,
        kb.HOME_BTN_MODE,
    ]
    for label in labels:
        assert i18n.localize_label(label, "ko") != label, (
            f"{label!r} falls back to English -- add it to i18n._LABELS"
        )


def test_catalog_miss_records_telemetry():
    i18n.register_auto_translator(None)
    i18n.clear_missing_translations()
    sentinel = "🟣 Unregistered Sentinel String"
    assert i18n.localize_text(sentinel, "ko") == sentinel  # English fallback
    assert ("ko", sentinel) in set(i18n.get_missing_translations())


def test_auto_translator_fallback_and_cache():
    calls = {"n": 0}

    def fake(text, lang):
        calls["n"] += 1
        return f"[{lang}] {text}"

    i18n.register_auto_translator(fake)
    try:
        s = "🟠 Another Unregistered String"
        first = i18n.localize_text(s, "ko")
        second = i18n.localize_text(s, "ko")
        assert first == f"[ko] {s}"
        assert second == first
        assert calls["n"] == 1  # second lookup served from cache
    finally:
        i18n.register_auto_translator(None)


def test_english_is_passthrough():
    assert i18n.localize_text("anything at all", "en") == "anything at all"
    assert i18n.localize_label("🔔 Alerts", "en") == "🔔 Alerts"


def test_update_user_language_persists_logs_and_audits(monkeypatch):
    """Switching language must persist (normalized) and leave an audit trail."""
    us = pytest.importorskip("src.nadobro.services.user_service")
    import src.nadobro.services.audit_log as audit_log

    recorded = {"updates": [], "audits": []}
    monkeypatch.setattr(us, "query_one", lambda sql, params=None: {"language": "en"})
    monkeypatch.setattr(us, "execute", lambda sql, params=None: recorded["updates"].append(params))
    monkeypatch.setattr(us, "invalidate_user_cache", lambda telegram_id: None)
    monkeypatch.setattr(
        audit_log, "record_audit_event",
        lambda *a, **k: recorded["audits"].append(a),
    )

    us.update_user_language(123, "ko-KR", source="settings")

    # Normalized to "ko" and written for the right user.
    assert recorded["updates"] == [("ko", 123)]
    # One audit row describing the transition.
    assert recorded["audits"], "no audit event recorded for language change"
    assert recorded["audits"][0][1] == "language_changed"


def test_get_or_create_user_seeds_from_device_locale(monkeypatch):
    """A brand-new user's row is seeded from the Telegram client locale."""
    us = pytest.importorskip("src.nadobro.services.user_service")

    inserts = []
    monkeypatch.setattr(us, "query_one", lambda sql, params=None: None)
    monkeypatch.setattr(us, "execute", lambda sql, params=None: inserts.append(params))
    monkeypatch.setattr(us, "_cache_user", lambda user: None)

    us.get_or_create_user(555, "trader", language_code="ko-KR")

    # The INSERT is the first execute; language (3rd positional) must be "ko".
    assert inserts, "no INSERT issued for new user"
    insert_params = inserts[0]
    assert insert_params[2] == "ko", f"expected seeded 'ko', got {insert_params!r}"
