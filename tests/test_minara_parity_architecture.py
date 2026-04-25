from _stubs import install_test_stubs

install_test_stubs()


def test_source_registry_records_freshness():
    from src.nadobro.services.source_registry import SourceRegistry

    registry = SourceRegistry()
    rec = registry.record("coinmarketcap", ttl_seconds=120, detail="quotes")

    assert rec.provider == "coinmarketcap"
    assert not rec.is_stale
    assert "quotes" in registry.freshness_footer()


def test_provider_catalog_contains_minara_equivalent_tools(monkeypatch):
    monkeypatch.setenv("DMIND_API_KEY", "dev")

    from src.nadobro.connectors.provider_catalog import provider_catalog

    names = {p["provider"] for p in provider_catalog()}
    assert {"dmind", "n8n", "arkham", "coinglass", "defillama", "glassnode", "rootdata", "goplus", "fmp"} <= names
    assert next(p for p in provider_catalog() if p["provider"] == "dmind")["configured"] is True


def test_dmind_degraded_mode_without_key(monkeypatch):
    monkeypatch.delenv("DMIND_API_KEY", raising=False)

    from src.nadobro.services.dmind_service import analyze_financial_context

    result = analyze_financial_context("score BTC", context="BTC context")
    assert result["ok"] is False
    assert result["degraded"] is True


def test_strategy_fsm_infers_failed_recoverable_state():
    from src.nadobro.services.strategy_fsm import PHASE_FAILED, infer_phase

    phase = infer_phase({"running": True, "last_error": "archive fill sync failed"})

    assert phase.phase == PHASE_FAILED
    assert phase.recoverable is True
    assert "recover" in phase.allowed_actions


def test_workflow_builder_selects_funding_template(monkeypatch):
    stored = {}

    def fake_set(key, value):
        stored[key] = value

    monkeypatch.setattr("src.nadobro.services.workflow_service.set_bot_state", fake_set)

    from src.nadobro.services.workflow_service import build_and_save_workflow

    result = build_and_save_workflow(123, "When BTC funding is above threshold recommend a grid")

    assert result["ok"] is True
    assert result["workflow"]["template_id"] == "funding_recommend_strategy"
    assert stored


def test_order_intent_suppresses_recent_duplicate(monkeypatch):
    store = {}

    def fake_get(key):
        return store.get(key)

    def fake_set(key, value):
        store[key] = value

    monkeypatch.setattr("src.nadobro.services.order_intents.get_bot_state", fake_get)
    monkeypatch.setattr("src.nadobro.services.order_intents.set_bot_state", fake_set)

    from src.nadobro.services.order_intents import create_order_intent, should_skip_duplicate

    create_order_intent("abc", {"trade_id": 7, "status": "pending"})
    skip, existing = should_skip_duplicate("abc")

    assert skip is True
    assert existing["trade_id"] == 7
