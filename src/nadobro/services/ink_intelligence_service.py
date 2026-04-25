"""Unified Ink-native market intelligence service."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.nadobro.connectors.provider_catalog import provider_catalog
from src.nadobro.services.dmind_service import analyze_financial_context, build_degraded_notice
from src.nadobro.services.source_registry import freshness_footer, record_source, source_health_snapshot


@dataclass
class IntelligenceRequest:
    query: str
    products: list[str]
    network: str = "mainnet"
    include_dmind: bool = True


def provider_status() -> dict[str, Any]:
    return {
        "providers": provider_catalog(),
        "source_health": source_health_snapshot(),
    }


def build_market_intelligence_snapshot(
    *,
    client=None,
    products: list[str] | None = None,
    network: str = "mainnet",
    include_dmind: bool = True,
    query: str = "Summarize current Nado/Ink market conditions.",
) -> dict[str, Any]:
    products = products or ["BTC", "ETH", "SOL"]
    snapshot: dict[str, Any] = {
        "network": network,
        "products": products,
        "market_snapshot": {},
        "provider_status": provider_status(),
        "dmind": {},
        "freshness": "",
    }

    try:
        from src.nadobro.services.market_scanner import build_market_snapshot, format_snapshot_for_llm

        if client is not None:
            market_snapshot = build_market_snapshot(client, products)
            snapshot["market_snapshot"] = market_snapshot
            record_source(
                "nado_market_scanner",
                ttl_seconds=120,
                confidence=0.85,
                detail="Nado market scanner snapshot",
                allowed_use="strategy_recommendation",
                metadata={"products": products, "network": network},
            )
            if include_dmind:
                context = format_snapshot_for_llm(market_snapshot)
                snapshot["dmind"] = analyze_financial_context(
                    query,
                    context=context,
                    task="ink_market_intelligence",
                    schema_hint={
                        "summary": "string",
                        "regime": "risk_on|neutral|risk_off",
                        "signals": ["string"],
                        "risks": ["string"],
                        "confidence": "0..1",
                    },
                )
        else:
            record_source(
                "nado_market_scanner",
                ttl_seconds=30,
                confidence=0.0,
                detail="Nado market scanner unavailable without client",
                allowed_use="analysis",
            )
            if include_dmind:
                snapshot["dmind"] = {
                    "ok": False,
                    "degraded": True,
                    "text": build_degraded_notice() or "No exchange client supplied for live Nado data.",
                }
    except Exception as exc:
        snapshot["error"] = str(exc)
        record_source(
            "ink_intelligence",
            ttl_seconds=30,
            confidence=0.0,
            detail="Ink intelligence snapshot failed",
            allowed_use="analysis",
        )

    snapshot["freshness"] = freshness_footer(limit=5)
    return snapshot
