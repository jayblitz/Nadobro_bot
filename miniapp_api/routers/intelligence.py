"""Market intelligence and provider freshness routes."""

from fastapi import APIRouter
from pydantic import BaseModel, Field

from miniapp_api.dependencies import AuthUser
from src.nadobro.services.async_utils import run_blocking
from src.nadobro.services.ink_intelligence_service import (
    build_market_intelligence_snapshot,
    provider_status,
)
from src.nadobro.services.user_service import get_user_readonly_client

router = APIRouter()


class IntelligenceRequest(BaseModel):
    products: list[str] = Field(default_factory=lambda: ["BTC", "ETH", "SOL"])
    query: str = "Summarize current Nado/Ink market conditions."
    include_dmind: bool = True


@router.get("/intelligence/providers")
async def intelligence_providers(user: AuthUser):
    return provider_status()


@router.post("/intelligence/snapshot")
async def intelligence_snapshot(body: IntelligenceRequest, user: AuthUser):
    client = await run_blocking(get_user_readonly_client, user.telegram_id, network=user.network)
    return await run_blocking(
        build_market_intelligence_snapshot,
        client=client,
        products=body.products,
        network=user.network,
        include_dmind=body.include_dmind,
        query=body.query,
    )
