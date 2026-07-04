"""Single LLM gateway — route all reasoning through NanoGPT (one API key, many
models) with graceful fallback to native XAI/OpenAI when NanoGPT is not
configured.

The user's NanoGPT subscription exposes Claude, GPT-5.5, DMind, etc. under one
key, so every reasoning surface (chat, morning brief, desk parse, edge/market
scan, finance analyst) should point here and select a model per task via env.

NanoGPT is OpenAI-compatible, so ``chat_client()`` returns an ``OpenAI`` SDK
client that is a drop-in for every existing ``.chat.completions.create(...)``
call site — the migration only swaps which client and which model string a
factory returns, never the call itself.

Per-task model env vars (set these to the exact model ids your NanoGPT plan
lists — e.g. ``claude-sonnet-4.5``, ``gpt-5.5``, ``dmind/dmind-1``):

    NANOGPT_MODEL_CHAT      general chat + Ask Nadobro          (default gpt)
    NANOGPT_MODEL_FINANCE   finance / analyst reasoning         (default dmind)
    NANOGPT_MODEL_BRIEF     morning brief / news synthesis
    NANOGPT_MODEL_INTENT    cheap/fast intent + JSON extraction
    NANOGPT_MODEL_SCAN      edge / market alpha scanning
    NANOGPT_MODEL           global default when a task is unset
"""

from __future__ import annotations

import os
from typing import Optional

try:
    from openai import OpenAI
except Exception:  # optional in degraded/test environments
    OpenAI = None  # type: ignore

from src.nadobro.services.provider_config import nanogpt_api_key
from src.nadobro.services.nanogpt_client import nanogpt_base_url

_client: Optional["OpenAI"] = None

# task -> (env var, built-in default). Defaults are conservative NanoGPT ids;
# the operator overrides them with the exact strings their plan exposes.
_TASK_MODEL_ENV: dict[str, tuple[str, str]] = {
    "chat": ("NANOGPT_MODEL_CHAT", "chatgpt-4o-latest"),
    "finance": ("NANOGPT_MODEL_FINANCE", "dmind/dmind-1"),
    "brief": ("NANOGPT_MODEL_BRIEF", "chatgpt-4o-latest"),
    "intent": ("NANOGPT_MODEL_INTENT", "gpt-4o-mini"),
    "scan": ("NANOGPT_MODEL_SCAN", "chatgpt-4o-latest"),
    "json": ("NANOGPT_MODEL_JSON", "chatgpt-4o-latest"),
}


def gateway_configured() -> bool:
    """True when NanoGPT is the active reasoning provider."""
    return bool(nanogpt_api_key()) and OpenAI is not None


def _timeout() -> float:
    try:
        from src.nadobro.services.provider_runtime import provider_timeout_seconds

        return provider_timeout_seconds("nanogpt", 90)
    except Exception:
        return 90.0


def chat_client() -> Optional["OpenAI"]:
    """OpenAI-SDK client pointed at NanoGPT. ``None`` when not configured, so
    callers fall back to their native XAI/OpenAI client."""
    global _client
    if _client is not None:
        return _client
    if not gateway_configured():
        return None
    _client = OpenAI(
        api_key=nanogpt_api_key(),
        base_url=nanogpt_base_url(),
        timeout=_timeout(),
    )
    return _client


def model_for(task: str, fallback: Optional[str] = None) -> str:
    """Resolve the NanoGPT model id for a task, honoring the per-task env var,
    then the global ``NANOGPT_MODEL`` override, then the built-in default."""
    env, default = _TASK_MODEL_ENV.get(task, ("", ""))
    if env:
        val = (os.environ.get(env) or "").strip()
        if val:
            return val
    global_default = (os.environ.get("NANOGPT_MODEL") or "").strip()
    return default or global_default or fallback or "chatgpt-4o-latest"


def reset_cache() -> None:
    """Test hook — drop the memoized client so env changes take effect."""
    global _client
    _client = None
