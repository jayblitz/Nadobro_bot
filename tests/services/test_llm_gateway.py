"""LLM gateway: NanoGPT-primary routing with per-task model selection and
graceful fallback. Verifies the consolidation contract without any network."""
from __future__ import annotations

import pytest

from src.nadobro.services import llm_gateway


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    # Start from a known-empty provider env each test.
    for var in (
        "NANOGPT_API_KEY", "NANO_GPT_API_KEY", "NANOGPT_MODEL",
        "NANOGPT_MODEL_CHAT", "NANOGPT_MODEL_FINANCE", "NANOGPT_MODEL_JSON",
        "NANOGPT_MODEL_SCAN", "XAI_API_KEY", "OPENAI_API_KEY",
    ):
        monkeypatch.delenv(var, raising=False)
    llm_gateway.reset_cache()
    yield
    llm_gateway.reset_cache()


def test_gateway_off_without_key():
    assert llm_gateway.gateway_configured() is False
    assert llm_gateway.chat_client() is None


def test_gateway_on_with_key(monkeypatch):
    monkeypatch.setenv("NANOGPT_API_KEY", "sk-test")
    assert llm_gateway.gateway_configured() is True
    client = llm_gateway.chat_client()
    assert client is not None
    # Points at NanoGPT, not x.ai / openai.
    assert "nano-gpt.com" in str(client.base_url)


def test_model_for_precedence(monkeypatch):
    # built-in default
    assert llm_gateway.model_for("finance") == "dmind/dmind-1"
    assert llm_gateway.model_for("chat") == "chatgpt-4o-latest"
    # global override beats built-in only where no per-task default exists;
    # a per-task env var beats everything.
    monkeypatch.setenv("NANOGPT_MODEL_FINANCE", "dmind/dmind-3")
    monkeypatch.setenv("NANOGPT_MODEL_CHAT", "claude-sonnet-4.5")
    assert llm_gateway.model_for("finance") == "dmind/dmind-3"
    assert llm_gateway.model_for("chat") == "claude-sonnet-4.5"


def test_unknown_task_falls_back(monkeypatch):
    monkeypatch.setenv("NANOGPT_MODEL", "gpt-5.5")
    assert llm_gateway.model_for("does-not-exist") == "gpt-5.5"


def test_bro_llm_prefers_gateway(monkeypatch):
    monkeypatch.setenv("NANOGPT_API_KEY", "sk-test")
    monkeypatch.setenv("XAI_API_KEY", "xai-should-not-win")
    llm_gateway.reset_cache()
    import src.nadobro.services.bro_llm as bro_llm
    client = bro_llm._get_client()
    assert client is not None and "nano-gpt.com" in str(client.base_url)


def test_knowledge_service_general_gateway_but_xsearch_native(monkeypatch):
    monkeypatch.setenv("NANOGPT_API_KEY", "sk-test")
    monkeypatch.setenv("XAI_API_KEY", "xai-native")
    llm_gateway.reset_cache()
    import src.nadobro.services.knowledge_service as ks
    ks._xai_client = None
    ks._openai_client = None
    # General reasoning -> gateway; model resolves to the gateway chat model.
    assert "nano-gpt.com" in str(ks._get_xai_client().base_url)
    assert ks._model_for("xai") == llm_gateway.model_for("chat")
    # X-search stays on native Grok (its extra_body is Grok-only).
    assert "x.ai" in str(ks._get_native_xai_client().base_url)


def test_knowledge_service_no_native_xai_without_key(monkeypatch):
    monkeypatch.setenv("NANOGPT_API_KEY", "sk-test")
    llm_gateway.reset_cache()
    import src.nadobro.services.knowledge_service as ks
    ks._xai_client = None
    # No XAI key -> no native client -> X-search path disabled, general still works.
    assert ks._get_native_xai_client() is None
    assert ks._get_xai_client() is not None
