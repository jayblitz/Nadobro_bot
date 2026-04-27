"""NanoGPT OpenAI-compatible chat API (https://nano-gpt.com/api)."""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import requests

logger = logging.getLogger(__name__)


def nanogpt_api_key() -> str:
    return (os.environ.get("NANOGPT_API_KEY") or os.environ.get("NANO_GPT_API_KEY") or "").strip()


def nanogpt_is_configured() -> bool:
    return bool(nanogpt_api_key())


def nanogpt_base_url() -> str:
    raw = (os.environ.get("NANOGPT_BASE_URL") or "https://nano-gpt.com/api/v1").strip().rstrip("/")
    if (os.environ.get("NANOGPT_USE_LEGACY_ENDPOINT") or "").strip().lower() in ("1", "true", "yes", "on"):
        if raw.endswith("/v1"):
            return raw[: -len("/v1")] + "/v1legacy"
    return raw


def nanogpt_default_model() -> str:
    return (os.environ.get("NANOGPT_MODEL") or "chatgpt-4o-latest").strip()


def openai_compatible_chat(
    *,
    base_url: str,
    api_key: str,
    model: str,
    messages: list[dict[str, Any]],
    temperature: float = 0.2,
    timeout: float = 90.0,
) -> tuple[bool, str, dict[str, Any]]:
    """POST /chat/completions. Returns (ok, assistant_text, raw_json)."""
    base = base_url.rstrip("/")
    url = f"{base}/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    body: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
    }
    try:
        resp = requests.post(url, headers=headers, json=body, timeout=timeout)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        logger.warning("openai_compatible_chat failed: %s", exc)
        return False, "", {}

    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        return False, "", payload
    msg = choices[0].get("message") if isinstance(choices[0], dict) else None
    if isinstance(msg, dict) and isinstance(msg.get("content"), str):
        return True, msg["content"], payload
    if isinstance(choices[0], dict) and isinstance(choices[0].get("text"), str):
        return True, choices[0]["text"], payload
    return False, "", payload


def nanogpt_chat_completion(
    messages: list[dict[str, Any]],
    *,
    model: str | None = None,
    temperature: float = 0.2,
    timeout: float = 90.0,
) -> tuple[bool, str, dict[str, Any]]:
    key = nanogpt_api_key()
    if not key:
        return False, "", {}
    m = (model or nanogpt_default_model()).strip()
    return openai_compatible_chat(
        base_url=nanogpt_base_url(),
        api_key=key,
        model=m,
        messages=messages,
        temperature=temperature,
        timeout=timeout,
    )


def extract_json_object(text: str) -> dict[str, Any] | None:
    """Parse a JSON object from LLM output; strips ```json fences if present."""
    raw = (text or "").strip()
    if not raw:
        return None
    if "```" in raw:
        for part in raw.split("```"):
            chunk = part.strip()
            if chunk.lower().startswith("json"):
                chunk = chunk[4:].strip()
            if chunk.startswith("{"):
                raw = chunk
                break
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else None
    except json.JSONDecodeError:
        pass
    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        try:
            obj = json.loads(raw[start : end + 1])
            return obj if isinstance(obj, dict) else None
        except json.JSONDecodeError:
            return None
    return None
