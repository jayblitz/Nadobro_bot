"""DMind financial expert LLM integration.

DMind is treated as the finance-specialist layer. Generic LLMs may provide
style and explanation, but market recommendations should pass through this
service or clearly disclose degraded mode.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

import requests

from src.nadobro.services.source_registry import record_source

logger = logging.getLogger(__name__)


DMIND_BASE_URL = os.environ.get("DMIND_BASE_URL", "https://api.dmind.ai").rstrip("/")
DMIND_MODEL = os.environ.get("DMIND_MODEL", "dmind-finance")
DMIND_TIMEOUT_SECONDS = float(os.environ.get("DMIND_TIMEOUT_SECONDS", "20"))


class DMindUnavailable(RuntimeError):
    pass


def is_dmind_configured() -> bool:
    return bool(os.environ.get("DMIND_API_KEY", ""))


def _headers() -> dict[str, str]:
    key = os.environ.get("DMIND_API_KEY", "")
    return {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }


def _extract_text(payload: dict[str, Any]) -> str:
    if not isinstance(payload, dict):
        return ""
    if isinstance(payload.get("text"), str):
        return payload["text"]
    if isinstance(payload.get("answer"), str):
        return payload["answer"]
    choices = payload.get("choices")
    if isinstance(choices, list) and choices:
        msg = choices[0].get("message") if isinstance(choices[0], dict) else None
        if isinstance(msg, dict) and isinstance(msg.get("content"), str):
            return msg["content"]
        if isinstance(choices[0].get("text"), str):
            return choices[0]["text"]
    return json.dumps(payload)[:4000]


def analyze_financial_context(
    prompt: str,
    *,
    context: str = "",
    task: str = "market_analysis",
    schema_hint: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run DMind finance analysis.

    Returns a stable envelope even when DMind is not configured so callers can
    disclose degraded mode rather than silently falling back.
    """
    if not is_dmind_configured():
        record_source(
            "dmind",
            ttl_seconds=30,
            confidence=0.0,
            source_url="https://dmind.ai/",
            detail="DMind not configured",
            allowed_use="finance_llm",
        )
        return {
            "ok": False,
            "degraded": True,
            "provider": "dmind",
            "error": "DMIND_API_KEY not configured",
            "text": "",
            "confidence": 0.0,
        }

    url = f"{DMIND_BASE_URL}/v1/chat/completions"
    body = {
        "model": DMIND_MODEL,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are Nadobro's finance-native expert model. Structure data, "
                    "summarize market context, score signals, and produce cautious "
                    "trading recommendations grounded only in provided sources."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "task": task,
                        "prompt": prompt,
                        "context": context[:20000],
                        "schema_hint": schema_hint or {},
                    },
                    ensure_ascii=True,
                ),
            },
        ],
        "temperature": 0.1,
    }
    started = time.perf_counter()
    try:
        resp = requests.post(url, headers=_headers(), json=body, timeout=DMIND_TIMEOUT_SECONDS)
        latency_ms = (time.perf_counter() - started) * 1000
        resp.raise_for_status()
        payload = resp.json()
        text = _extract_text(payload)
        record_source(
            "dmind",
            ttl_seconds=300,
            confidence=0.9,
            latency_ms=latency_ms,
            source_url="https://dmind.ai/",
            detail=f"DMind {task}",
            allowed_use="finance_llm",
            metadata={"model": DMIND_MODEL},
        )
        return {
            "ok": True,
            "degraded": False,
            "provider": "dmind",
            "model": DMIND_MODEL,
            "text": text,
            "raw": payload,
            "confidence": 0.9,
        }
    except Exception as exc:
        latency_ms = (time.perf_counter() - started) * 1000
        logger.warning("DMind request failed: %s", exc)
        record_source(
            "dmind",
            ttl_seconds=30,
            confidence=0.0,
            latency_ms=latency_ms,
            source_url="https://dmind.ai/",
            detail="DMind request failed",
            allowed_use="finance_llm",
        )
        return {
            "ok": False,
            "degraded": True,
            "provider": "dmind",
            "error": str(exc),
            "text": "",
            "confidence": 0.0,
        }


def build_degraded_notice() -> str:
    if is_dmind_configured():
        return ""
    return "DMind finance expert is not configured; this response is in degraded mode."
