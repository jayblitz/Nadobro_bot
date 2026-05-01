from src.nadobro.services import bot_runtime


def test_bro_dispatch_skips_when_legacy_disabled(monkeypatch):
    monkeypatch.setattr("src.nadobro.services.bot_runtime.legacy_bro_autoloop_enabled", lambda: False)
    result = bot_runtime._dispatch_strategy("bro", 1, "mainnet", {}, None, 0, 1, "BTC", [])
    assert result["reason"] == "legacy_bro_autoloop_disabled"


def test_bro_start_rejected_when_legacy_disabled(monkeypatch):
    monkeypatch.setattr("src.nadobro.services.bot_runtime.legacy_bro_autoloop_enabled", lambda: False)
    ok, msg = bot_runtime.start_user_bot(1, "bro", "BTC")
    assert ok is False
    assert "Strategy Studio" in msg
