"""Signal analyst — overlay-signal summary + DMind-backed recommendations with
a deterministic fallback, and its integration into the Night HOWL report."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.nadobro.llm import signal_analyst as sa


def _rows():
    return [
        {"regime": "trend_up", "bias": 0.6, "confidence": 0.8, "scale": 0.5,
         "action_json": {"suppress_new_entries": False}},
        {"regime": "trend_up", "bias": 0.7, "confidence": 0.85, "scale": 0.6, "action_json": {}},
        {"regime": "chop", "bias": 0.0, "confidence": 0.2, "scale": 0.0,
         "action_json": {"suppress_new_entries": True}},
        {"regime": "range", "bias": -0.1, "confidence": 0.4, "scale": -0.3, "action_json": {}},
    ]


def test_summarize_overlay_signals():
    s = sa.summarize_overlay_signals(_rows())
    assert s["signals"] == 4
    assert s["dominant_regime"] == "trend_up"
    assert s["suppressed"] == 1
    assert s["scaled_up"] == 2 and s["scaled_down"] == 1
    assert -1.0 <= s["avg_bias"] <= 1.0
    assert 0.0 <= s["avg_confidence"] <= 1.0


def test_summarize_empty():
    s = sa.summarize_overlay_signals([])
    assert s["signals"] == 0 and s["dominant_regime"] is None


def test_analyze_activity_falls_back_without_finance_llm(monkeypatch):
    monkeypatch.delenv("NANOGPT_API_KEY", raising=False)
    monkeypatch.delenv("NANO_GPT_API_KEY", raising=False)
    monkeypatch.delenv("DMIND_API_KEY", raising=False)
    pattern = {"trades": 5, "volume_usd": 1000, "fees_usd": 2, "realized_pnl_usd": 10,
               "net_pnl_usd": 8, "win_rate": 0.6, "top_pairs": [{"pair": "BTC-PERP", "volume_usd": 1000}]}
    summary = sa.summarize_overlay_signals(_rows())
    res = sa.analyze_activity(pattern, summary)
    assert res["degraded"] is True
    assert res["provider"] == "none"
    assert len(res["recommendations"]) >= 1
    # The overlay note is appended to the deterministic recs.
    assert any("Overlay read" in r for r in res["recommendations"])


def test_analyze_activity_uses_llm_when_configured(monkeypatch):
    monkeypatch.setenv("NANOGPT_API_KEY", "sk-test")
    import src.nadobro.llm.dmind_service as dmind

    monkeypatch.setattr(dmind, "is_finance_expert_configured", lambda: True)
    monkeypatch.setattr(dmind, "analyze_financial_context", lambda *a, **k: {
        "ok": True, "provider": "nanogpt",
        "text": '{"recommendations":["Cut taker fills on BTC.","Hold size in chop."],'
                '"risks":["Funding turning against longs."],"narrative":"Clean uptrend day."}',
    })
    pattern = {"trades": 5, "volume_usd": 1000, "net_pnl_usd": 8, "top_pairs": []}
    res = sa.analyze_activity(pattern, sa.summarize_overlay_signals(_rows()))
    assert res["degraded"] is False
    assert res["provider"] == "nanogpt"
    assert res["recommendations"] == ["Cut taker fills on BTC.", "Hold size in chop."]
    assert res["risks"] == ["Funding turning against longs."]
    assert res["narrative"] == "Clean uptrend day."


def test_analyze_activity_llm_bad_shape_keeps_fallback(monkeypatch):
    monkeypatch.setenv("NANOGPT_API_KEY", "sk-test")
    import src.nadobro.llm.dmind_service as dmind

    monkeypatch.setattr(dmind, "is_finance_expert_configured", lambda: True)
    monkeypatch.setattr(dmind, "analyze_financial_context", lambda *a, **k: {
        "ok": True, "provider": "nanogpt", "text": "not json at all",
    })
    pattern = {"trades": 3, "volume_usd": 500, "net_pnl_usd": 2, "top_pairs": []}
    res = sa.analyze_activity(pattern, sa.summarize_overlay_signals(_rows()))
    # LLM answered but unparseable -> deterministic recs retained.
    assert len(res["recommendations"]) >= 1


def test_night_howl_report_includes_overlay_section(monkeypatch):
    import src.nadobro.models.database as db
    import src.nadobro.services.bot_runtime as br
    import src.nadobro.llm.night_howl_service as nh

    now = datetime(2026, 7, 4, 12, 0, tzinfo=timezone.utc)
    # One venue-confirmed fill in-window so the report is non-empty.
    monkeypatch.setattr(db, "get_trades_by_user", lambda *a, **k: [{
        "created_at": now.timestamp() - 3600, "product_id": 2, "product_name": "BTC-PERP",
        "side": "long", "fill_size": 0.01, "fill_price": 60000, "submission_idx": 1, "status": "filled",
    }])
    monkeypatch.setattr(db, "get_account_realized_pnl_windows", lambda *a, **k: {})
    monkeypatch.setattr(br, "_load_state", lambda *a, **k: {})
    # Overlay had a trending-up session.
    monkeypatch.setattr(db, "get_overlay_signals", lambda *a, **k: [
        {"regime": "trend_up", "bias": 0.6, "confidence": 0.8, "scale": 0.5, "action_json": {}},
        {"regime": "trend_up", "bias": 0.7, "confidence": 0.82, "scale": 0.6, "action_json": {}},
    ])
    memstore: dict = {}
    monkeypatch.setattr(nh, "save_report", lambda *a, **k: memstore.update({"saved": True}))

    report = nh.build_report(7, "mainnet", now_utc=now)
    assert report is not None
    assert report["signal_summary"]["dominant_regime"] == "trend_up"
    assert "Overlay read" in report["markdown"]
    assert "trending up" in report["markdown"]
