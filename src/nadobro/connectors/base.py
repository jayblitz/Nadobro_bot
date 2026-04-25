"""Shared connector primitives for external data providers."""

from __future__ import annotations

from dataclasses import dataclass, field
import os
import time
from typing import Any

import requests

from src.nadobro.services.source_registry import SourceRecord, record_source


@dataclass
class ProviderResponse:
    provider: str
    data: Any
    source: SourceRecord
    ok: bool = True
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "ok": self.ok,
            "error": self.error,
            "data": self.data,
            "source": self.source.to_dict(),
        }


@dataclass
class ProviderConnector:
    provider: str
    base_url: str = ""
    api_key_env: str = ""
    source_url: str = ""
    default_ttl_seconds: int = 300
    license_tier: str = "unknown"
    allowed_use: str = "analysis"
    timeout_seconds: float = 8.0
    headers: dict[str, str] = field(default_factory=dict)

    @property
    def api_key(self) -> str:
        return os.environ.get(self.api_key_env, "") if self.api_key_env else ""

    def is_configured(self) -> bool:
        return not self.api_key_env or bool(self.api_key)

    def record(
        self,
        *,
        ttl_seconds: int | None = None,
        confidence: float = 1.0,
        latency_ms: float = 0.0,
        source_url: str = "",
        detail: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> SourceRecord:
        return record_source(
            self.provider,
            ttl_seconds=ttl_seconds or self.default_ttl_seconds,
            confidence=confidence,
            latency_ms=latency_ms,
            source_url=source_url or self.source_url,
            license_tier=self.license_tier,
            allowed_use=self.allowed_use,
            detail=detail,
            metadata=metadata or {},
        )

    def unavailable(self, reason: str) -> ProviderResponse:
        source = self.record(confidence=0.0, ttl_seconds=30, detail=f"{self.provider} unavailable")
        return ProviderResponse(provider=self.provider, data={}, source=source, ok=False, error=reason)

    def get_json(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        ttl_seconds: int | None = None,
        detail: str = "",
    ) -> ProviderResponse:
        if not self.base_url:
            return self.unavailable("base URL not configured")
        if not self.is_configured():
            return self.unavailable(f"{self.api_key_env} not configured")
        url = self.base_url.rstrip("/") + "/" + path.lstrip("/")
        request_headers = dict(self.headers)
        request_headers.update(headers or {})
        started = time.perf_counter()
        try:
            resp = requests.get(url, params=params or {}, headers=request_headers, timeout=self.timeout_seconds)
            latency_ms = (time.perf_counter() - started) * 1000
            resp.raise_for_status()
            data = resp.json()
            source = self.record(
                ttl_seconds=ttl_seconds,
                latency_ms=latency_ms,
                source_url=url,
                detail=detail or self.provider,
                metadata={"params": params or {}},
            )
            return ProviderResponse(provider=self.provider, data=data, source=source)
        except Exception as exc:
            latency_ms = (time.perf_counter() - started) * 1000
            source = self.record(
                ttl_seconds=30,
                confidence=0.0,
                latency_ms=latency_ms,
                source_url=url,
                detail=f"{self.provider} error",
                metadata={"params": params or {}},
            )
            return ProviderResponse(provider=self.provider, data={}, source=source, ok=False, error=str(exc))
