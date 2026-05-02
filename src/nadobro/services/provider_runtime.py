"""Shared timeout/retry helpers for external provider calls."""

from __future__ import annotations

import logging
import os
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)


def provider_timeout_seconds(provider: str, default: float) -> float:
    key = f"{provider.upper()}_TIMEOUT_SECONDS"
    try:
        return max(1.0, float((os.environ.get(key) or "").strip() or default))
    except (TypeError, ValueError):
        return float(default)


def provider_retry_count(provider: str, default: int = 1) -> int:
    key = f"{provider.upper()}_RETRY_COUNT"
    try:
        return max(0, min(5, int((os.environ.get(key) or "").strip() or default)))
    except (TypeError, ValueError):
        return int(default)


def record_provider_degraded(
    provider: str,
    detail: str,
    *,
    latency_ms: float = 0.0,
    allowed_use: str = "provider",
    source_url: str = "",
) -> None:
    try:
        from src.nadobro.services.source_registry import record_source

        record_source(
            provider,
            ttl_seconds=30,
            confidence=0.0,
            latency_ms=latency_ms,
            source_url=source_url,
            detail=detail,
            allowed_use=allowed_use,
        )
    except Exception:
        logger.debug("Failed to record degraded provider=%s", provider, exc_info=True)


def post_json_with_retries(
    provider: str,
    url: str,
    *,
    headers: dict[str, str],
    json_body: dict[str, Any],
    timeout: float,
    retries: int | None = None,
) -> tuple[requests.Response, float]:
    attempts = 1 + (provider_retry_count(provider) if retries is None else max(0, int(retries)))
    last_exc: Exception | None = None
    started = time.perf_counter()
    for attempt in range(attempts):
        try:
            resp = requests.post(url, headers=headers, json=json_body, timeout=timeout)
            status_code = int(getattr(resp, "status_code", 200) or 200)
            if status_code < 500 and status_code != 429:
                return resp, (time.perf_counter() - started) * 1000
            last_exc = requests.HTTPError(f"{provider} HTTP {status_code}")
        except requests.RequestException as exc:
            last_exc = exc
        if attempt + 1 < attempts:
            time.sleep(min(0.25 * (2 ** attempt), 1.0))
    if last_exc:
        raise last_exc
    raise RuntimeError(f"{provider} request failed")
