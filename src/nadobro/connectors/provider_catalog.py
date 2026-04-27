"""Catalog of Minara-equivalent data providers targeted by Nadobro."""

from __future__ import annotations

from dataclasses import dataclass

from src.nadobro.services.provider_config import provider_configured


@dataclass(frozen=True)
class ProviderSpec:
    provider: str
    api_key_env: str
    category: str
    expected_latency: str
    primary_use: str
    source_url: str = ""

    @property
    def configured(self) -> bool:
        return provider_configured(self.provider, self.api_key_env)

    def to_dict(self) -> dict:
        return {
            "provider": self.provider,
            "api_key_env": self.api_key_env,
            "configured": self.configured,
            "category": self.category,
            "expected_latency": self.expected_latency,
            "primary_use": self.primary_use,
            "source_url": self.source_url,
        }


MINARA_EQUIVALENT_PROVIDERS: tuple[ProviderSpec, ...] = (
    ProviderSpec("nanogpt", "NANOGPT_API_KEY", "financial_llm", "real-time", "NanoGPT router: finance reasoning and workflow JSON (OpenAI-compatible)", "https://nano-gpt.com/api"),
    ProviderSpec("dmind", "DMIND_API_KEY", "financial_llm", "real-time", "financial structuring, summarization, signal scoring (optional if NanoGPT set)", "https://dmind.ai/"),
    ProviderSpec("n8n", "N8N_API_KEY", "workflow", "real-time", "workflow creation, deployment, run status", "https://n8n.io/"),
    ProviderSpec("ink_rpc", "INK_RPC_URL", "blockchain_rpc", "real-time", "Ink blocks, logs, wallet/protocol events"),
    ProviderSpec("arkham", "ARKHAM_API_KEY", "onchain_intelligence", "real-time", "entities, capital flow, whale tracking", "https://arkhamintelligence.com/"),
    ProviderSpec("coinmarketcap", "CMC_API_KEY", "market_data", "real-time", "quotes, metadata, global market, fear/greed", "https://coinmarketcap.com/"),
    ProviderSpec("coingecko", "COINGECKO_API_KEY", "market_data", "real-time", "quotes, metadata, trending fallback", "https://www.coingecko.com/"),
    ProviderSpec("coinglass", "COINGLASS_API_KEY", "derivatives", "real-time", "funding, open interest, liquidations, ETF/perp signals", "https://www.coinglass.com/"),
    ProviderSpec("defillama", "DEFILLAMA_API_KEY", "defi_fundamentals", "real-time", "TVL, volume, stablecoin flows, yield context", "https://defillama.com/"),
    ProviderSpec("glassnode", "GLASSNODE_API_KEY", "onchain_market", "real-time", "on-chain and market intelligence", "https://glassnode.com/"),
    ProviderSpec("nftgo", "NFTGO_API_KEY", "nft_market", "real-time", "NFT data and sentiment", "https://nftgo.io/"),
    ProviderSpec("rootdata", "ROOTDATA_API_KEY", "project_intelligence", "1min", "project/team/funding/news intelligence", "https://www.rootdata.com/"),
    ProviderSpec("x", "X_API_BEARER_TOKEN", "social", "real-time", "social posts, Nado/Ink accounts, engagement"),
    ProviderSpec("global_news", "GLOBAL_NEWS_API_KEY", "news", "5-10s", "macro, policy, regulatory, market-moving news"),
    ProviderSpec("polymarket", "POLYMARKET_API_KEY", "prediction_market", "real-time", "event probabilities and odds changes", "https://polymarket.com/"),
    ProviderSpec("goplus", "GOPLUS_API_KEY", "risk_safety", "real-time", "token/address/contract safety checks", "https://gopluslabs.io/"),
    ProviderSpec("fmp", "FMP_API_KEY", "tradfi", "real-time", "public equities, ETF, macro and traditional finance data", "https://site.financialmodelingprep.com/"),
    ProviderSpec("openai", "OPENAI_API_KEY", "web_search", "real-time", "supplementary search/scraping only"),
    ProviderSpec("grok", "XAI_API_KEY", "web_search", "real-time", "supplementary search and social-aware reasoning"),
)


def provider_catalog() -> list[dict]:
    return [p.to_dict() for p in MINARA_EQUIVALENT_PROVIDERS]


def configured_provider_names() -> list[str]:
    return [p.provider for p in MINARA_EQUIVALENT_PROVIDERS if p.configured]
