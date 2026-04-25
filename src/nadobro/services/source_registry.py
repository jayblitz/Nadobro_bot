"""Source metadata and freshness tracking for Nadobro intelligence.

The goal is to make every market/AI answer auditable: which provider supplied
the data, when it was fetched, how long it remains fresh, and whether it is
allowed to drive actionable recommendations.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import time
from typing import Any


@dataclass(frozen=True)
class SourceRecord:
    provider: str
    fetched_at: float
    ttl_seconds: int
    confidence: float = 1.0
    latency_ms: float = 0.0
    source_url: str = ""
    license_tier: str = "unknown"
    allowed_use: str = "analysis"
    detail: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def age_seconds(self) -> float:
        return max(0.0, time.time() - float(self.fetched_at or 0.0))

    @property
    def is_stale(self) -> bool:
        return self.age_seconds > max(0, int(self.ttl_seconds or 0))

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["age_seconds"] = self.age_seconds
        data["is_stale"] = self.is_stale
        data["fetched_at_iso"] = datetime.fromtimestamp(
            self.fetched_at, tz=timezone.utc
        ).isoformat()
        return data


class SourceRegistry:
    """In-memory source ledger for the current process.

    This intentionally starts process-local so the product can expose freshness
    quickly without adding a migration. Workflow/audit persistence can consume
    the same serialised records later.
    """

    def __init__(self, max_records: int = 500):
        self.max_records = max(50, int(max_records))
        self._records: list[SourceRecord] = []

    def record(
        self,
        provider: str,
        *,
        ttl_seconds: int,
        confidence: float = 1.0,
        latency_ms: float = 0.0,
        source_url: str = "",
        license_tier: str = "unknown",
        allowed_use: str = "analysis",
        detail: str = "",
        metadata: dict[str, Any] | None = None,
        fetched_at: float | None = None,
    ) -> SourceRecord:
        rec = SourceRecord(
            provider=str(provider),
            fetched_at=float(fetched_at or time.time()),
            ttl_seconds=int(ttl_seconds),
            confidence=max(0.0, min(1.0, float(confidence))),
            latency_ms=max(0.0, float(latency_ms or 0.0)),
            source_url=str(source_url or ""),
            license_tier=str(license_tier or "unknown"),
            allowed_use=str(allowed_use or "analysis"),
            detail=str(detail or ""),
            metadata=dict(metadata or {}),
        )
        self._records.append(rec)
        if len(self._records) > self.max_records:
            self._records = self._records[-self.max_records :]
        return rec

    def recent(self, limit: int = 10, provider: str | None = None) -> list[SourceRecord]:
        rows = self._records
        if provider:
            rows = [r for r in rows if r.provider == provider]
        return list(reversed(rows[-max(1, int(limit)) :]))

    def health_snapshot(self) -> dict[str, Any]:
        providers: dict[str, dict[str, Any]] = {}
        for rec in self._records:
            info = providers.setdefault(
                rec.provider,
                {
                    "provider": rec.provider,
                    "records": 0,
                    "latest": None,
                    "stale_records": 0,
                },
            )
            info["records"] += 1
            if rec.is_stale:
                info["stale_records"] += 1
            if not info["latest"] or rec.fetched_at > info["latest"]["fetched_at"]:
                info["latest"] = rec.to_dict()
        return {
            "providers": providers,
            "total_records": len(self._records),
            "generated_at": time.time(),
        }

    def freshness_footer(self, limit: int = 3) -> str:
        records = self.recent(limit=limit)
        if not records:
            return ""
        parts = []
        for rec in records:
            age = int(rec.age_seconds)
            status = "stale" if rec.is_stale else "fresh"
            label = rec.detail or rec.provider
            parts.append(f"{label}: {status}, {age}s old")
        return "Data freshness: " + " | ".join(parts)


GLOBAL_SOURCE_REGISTRY = SourceRegistry()


def record_source(provider: str, **kwargs) -> SourceRecord:
    return GLOBAL_SOURCE_REGISTRY.record(provider, **kwargs)


def recent_sources(limit: int = 10) -> list[dict[str, Any]]:
    return [r.to_dict() for r in GLOBAL_SOURCE_REGISTRY.recent(limit=limit)]


def source_health_snapshot() -> dict[str, Any]:
    return GLOBAL_SOURCE_REGISTRY.health_snapshot()


def freshness_footer(limit: int = 3) -> str:
    return GLOBAL_SOURCE_REGISTRY.freshness_footer(limit=limit)
