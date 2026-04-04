"""NadoBro Mini App API — FastAPI gateway.

This service wraps the existing nadobro service layer and exposes it as a
REST + WebSocket API for the Telegram Mini App frontend.
"""

import logging
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from miniapp_api.asgi_rate_limit import RateLimitMiddleware
from miniapp_api.config import MINIAPP_API_PORT, MINIAPP_CORS_ORIGINS

# Import routers
from miniapp_api.routers import users, market, trade, positions, candles, quotes, strategies
from miniapp_api.ws import voice

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("miniapp_api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialise shared resources on startup; tear down on shutdown."""
    # Initialise the database connection pool (shared with bot services).
    from src.nadobro.db import get_pool

    logger.info("Initialising database pool...")
    get_pool()
    try:
        from miniapp_api.rate_limit import ensure_rate_limit_table

        ensure_rate_limit_table()
        logger.info("miniapp_rate_limit table ready")
    except Exception:
        logger.exception("Could not ensure miniapp_rate_limit table (rate limiting may fail)")
    logger.info("Mini App API ready")

    yield

    logger.info("Mini App API shutting down")


app = FastAPI(
    title="NadoBro Mini App API",
    version="0.1.0",
    lifespan=lifespan,
)

# ---------------------------------------------------------------------------
# CORS — allow the Vercel-hosted frontend and Telegram WebApp domains.
# ---------------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=MINIAPP_CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# Outermost: rate limit mutating /api/* (last added runs first in Starlette).
app.add_middleware(RateLimitMiddleware)

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
app.include_router(users.router, prefix="/api", tags=["users"])
app.include_router(market.router, prefix="/api", tags=["market"])
app.include_router(candles.router, prefix="/api", tags=["candles"])
app.include_router(quotes.router, prefix="/api", tags=["quotes"])
app.include_router(trade.router, prefix="/api", tags=["trade"])
app.include_router(positions.router, prefix="/api", tags=["positions"])
app.include_router(strategies.router, prefix="/api", tags=["strategies"])
app.include_router(voice.router, tags=["voice"])


@app.get("/health")
async def health():
    return {"ok": True, "service": "nadobro-miniapp-api"}


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "miniapp_api.main:app",
        host="0.0.0.0",
        port=MINIAPP_API_PORT,
        reload=os.environ.get("MINIAPP_DEV", "").lower() in ("1", "true"),
    )
