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
    assert {"nanogpt", "dmind", "n8n", "arkham", "coinglass", "defillama", "glassnode", "rootdata", "goplus", "fmp"} <= names
    assert next(p for p in provider_catalog() if p["provider"] == "dmind")["configured"] is True


def test_dmind_degraded_mode_without_key(monkeypatch):
    monkeypatch.delenv("DMIND_API_KEY", raising=False)
    monkeypatch.delenv("NANOGPT_API_KEY", raising=False)
    monkeypatch.delenv("NANO_GPT_API_KEY", raising=False)

    from src.nadobro.services.dmind_service import analyze_financial_context

    result = analyze_financial_context("score BTC", context="BTC context")
    assert result["ok"] is False
    assert result["degraded"] is True


def test_nanogpt_json_extract_strips_fences():
    from src.nadobro.services.nanogpt_client import extract_json_object

    raw = '```json\n{"a": 1}\n```'
    assert extract_json_object(raw) == {"a": 1}


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

    monkeypatch.delenv("NANOGPT_API_KEY", raising=False)
    monkeypatch.delenv("NANO_GPT_API_KEY", raising=False)
    monkeypatch.delenv("N8N_WORKFLOWS_USE_LLM", raising=False)
    monkeypatch.setattr("src.nadobro.services.workflow_service.set_bot_state", fake_set)

    from src.nadobro.services.workflow_service import build_and_save_workflow

    result = build_and_save_workflow(123, "When BTC funding is above threshold recommend a grid")

    assert result["ok"] is True
    assert result["workflow"]["template_id"] == "funding_recommend_strategy"
    assert result["workflow"]["connections"]
    assert result["workflow"]["nodes"][0]["type"] == "n8n-nodes-base.manualTrigger"
    assert stored


def test_n8n_deploy_resolves_fly_style_secrets(monkeypatch):
    monkeypatch.setenv("n8n_Server_URL", "https://n8n.example.test")
    monkeypatch.setenv("n8n_authorization", "Bearer test-token")
    captured: dict = {}

    def fake_post(url, headers=None, json=None, timeout=None):
        captured["url"] = url
        captured["headers"] = dict(headers or {})
        class Resp:
            def raise_for_status(self):
                return None

            def json(self):
                return {"id": "wf-99"}

        return Resp()

    monkeypatch.setattr("src.nadobro.services.workflow_service.requests.post", fake_post)
    from src.nadobro.services.workflow_service import deploy_to_n8n

    out = deploy_to_n8n(
        {"id": "abc", "name": "T", "nodes": [], "connections": {}, "template_id": "t"}
    )
    assert out["ok"] is True
    assert captured["url"] == "https://n8n.example.test/api/v1/workflows"
    assert captured["headers"].get("Authorization") == "Bearer test-token"
    assert captured["headers"].get("X-N8N-API-KEY") is None


def test_workflow_builder_llm_path(monkeypatch):
    monkeypatch.setenv("NANOGPT_API_KEY", "test-key")

    def fake_complete(messages, **kwargs):
        _ = messages
        body = {
            "name": "LLM WF",
            "nodes": [
                {
                    "id": "a",
                    "name": "Start",
                    "type": "n8n-nodes-base.manualTrigger",
                    "typeVersion": 1,
                    "position": [0, 0],
                    "parameters": {},
                },
                {
                    "id": "b",
                    "name": "Done",
                    "type": "n8n-nodes-base.set",
                    "typeVersion": 3.4,
                    "position": [200, 0],
                    "parameters": {"assignments": {"assignments": []}},
                },
            ],
            "connections": {"Start": {"main": [[{"node": "Done", "type": "main", "index": 0}]]}},
            "settings": {"timezone": "UTC"},
            "setup_guide": "Test guide",
        }
        import json

        return True, json.dumps(body), {}

    monkeypatch.setattr("src.nadobro.services.workflow_service.nanogpt_chat_completion", fake_complete)

    from src.nadobro.services.workflow_service import build_workflow_from_prompt

    wf = build_workflow_from_prompt("Alert me when ETH funding is extreme")
    assert wf["template_id"] == "llm_generated"
    assert wf["name"] == "LLM WF"
    assert wf["connections"]["Start"]["main"][0][0]["node"] == "Done"


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
