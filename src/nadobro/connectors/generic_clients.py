"""Lightweight clients for Minara-equivalent external data providers.

These clients intentionally expose small, normalized methods first. Provider
coverage can deepen endpoint-by-endpoint without changing the rest of Nadobro.
"""

from __future__ import annotations

import os
from typing import Any

from src.nadobro.connectors.base import ProviderConnector, ProviderResponse


class CoinGeckoClient(ProviderConnector):
    def __init__(self):
        headers = {}
        key = os.environ.get("COINGECKO_API_KEY", "")
        if key:
            headers["x-cg-demo-api-key"] = key
        super().__init__(
            provider="coingecko",
            base_url=os.environ.get("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3"),
            api_key_env="",
            source_url="https://www.coingecko.com/",
            default_ttl_seconds=120,
            allowed_use="analysis",
            headers=headers,
        )

    def simple_price(self, ids: list[str], vs_currency: str = "usd") -> ProviderResponse:
        return self.get_json(
            "/simple/price",
            params={"ids": ",".join(ids), "vs_currencies": vs_currency, "include_24hr_change": "true"},
            detail="CoinGecko simple price",
        )


class DeFiLlamaClient(ProviderConnector):
    def __init__(self):
        super().__init__(
            provider="defillama",
            base_url=os.environ.get("DEFILLAMA_BASE_URL", "https://api.llama.fi"),
            api_key_env="",
            source_url="https://defillama.com/",
            default_ttl_seconds=300,
        )

    def protocol(self, slug: str) -> ProviderResponse:
        return self.get_json(f"/protocol/{slug}", detail=f"DeFiLlama protocol {slug}")

    def chains(self) -> ProviderResponse:
        return self.get_json("/v2/chains", detail="DeFiLlama chains")


class FMPClient(ProviderConnector):
    def __init__(self):
        super().__init__(
            provider="fmp",
            base_url=os.environ.get("FMP_BASE_URL", "https://financialmodelingprep.com/api/v3"),
            api_key_env="FMP_API_KEY",
            source_url="https://site.financialmodelingprep.com/",
            default_ttl_seconds=300,
        )

    def quote(self, symbol: str) -> ProviderResponse:
        return self.get_json(f"/quote/{symbol}", params={"apikey": self.api_key}, detail=f"FMP quote {symbol}")


class GoPlusClient(ProviderConnector):
    def __init__(self):
        super().__init__(
            provider="goplus",
            base_url=os.environ.get("GOPLUS_BASE_URL", "https://api.gopluslabs.io/api/v1"),
            api_key_env="",
            source_url="https://gopluslabs.io/",
            default_ttl_seconds=600,
            allowed_use="risk",
        )

    def token_security(self, chain_id: str, contract_addresses: list[str]) -> ProviderResponse:
        return self.get_json(
            f"/token_security/{chain_id}",
            params={"contract_addresses": ",".join(contract_addresses)},
            detail="GoPlus token security",
        )


class PolymarketClient(ProviderConnector):
    def __init__(self):
        super().__init__(
            provider="polymarket",
            base_url=os.environ.get("POLYMARKET_BASE_URL", "https://gamma-api.polymarket.com"),
            api_key_env="",
            source_url="https://polymarket.com/",
            default_ttl_seconds=120,
        )

    def search_markets(self, query: str, limit: int = 10) -> ProviderResponse:
        return self.get_json("/markets", params={"q": query, "limit": limit}, detail=f"Polymarket {query}")


class HeaderApiKeyClient(ProviderConnector):
    """Generic client for providers whose exact endpoints are configured later."""

    def __init__(self, provider: str, base_url_env: str, api_key_env: str, source_url: str):
        key = os.environ.get(api_key_env, "")
        headers = {"Authorization": f"Bearer {key}"} if key else {}
        super().__init__(
            provider=provider,
            base_url=os.environ.get(base_url_env, ""),
            api_key_env=api_key_env,
            source_url=source_url,
            default_ttl_seconds=300,
            headers=headers,
        )

    def configured_endpoint(self, path: str, params: dict[str, Any] | None = None, detail: str = "") -> ProviderResponse:
        return self.get_json(path, params=params or {}, detail=detail or self.provider)
