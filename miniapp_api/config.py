import os

# Mini App specific configuration.  Inherits shared values from the main
# nadobro config so that the API gateway and the bot share the same product
# catalog, DB connection string, etc.

from src.nadobro.config import (  # noqa: F401 — re-exported for convenience
    DATABASE_URL,
    ENCRYPTION_KEY,
    EST_FEE_RATE,
    MAX_LEVERAGE,
    MIN_TRADE_SIZE_USD,
    PRODUCTS,
    PRODUCT_ALIASES,
    PRODUCT_MAX_LEVERAGE,
    RATE_LIMIT_SECONDS,
    SPOT_PRODUCT_IDS,
    TELEGRAM_TOKEN,
    get_perp_products,
    get_product_id,
    get_product_max_leverage,
    get_product_name,
    is_product_isolated_only,
)

# ---------------------------------------------------------------------------
# Mini-app-only settings
# ---------------------------------------------------------------------------


def _load_cors_origins() -> list[str]:
    raw = [
        o.strip()
        for o in os.environ.get(
            "MINIAPP_CORS_ORIGINS",
            "https://web-app.telegram.org,https://tg.dev",
        ).split(",")
        if o.strip()
    ]
    # Reject "*": CORSMiddleware with allow_credentials=True cannot use wildcard origins safely.
    return [o for o in raw if o != "*"]


MINIAPP_CORS_ORIGINS: list[str] = _load_cors_origins()

# Allow browser calls from the same host as the SPA (e.g. Fly URL) when testing outside Telegram iframe.
_miniapp_origin = (os.environ.get("MINIAPP_URL") or "").strip().rstrip("/")
if _miniapp_origin.startswith("https://") and _miniapp_origin not in MINIAPP_CORS_ORIGINS:
    MINIAPP_CORS_ORIGINS = [*MINIAPP_CORS_ORIGINS, _miniapp_origin]

MINIAPP_API_PORT: int = int(os.environ.get("MINIAPP_API_PORT", "8081"))

# Gemini Live API
GEMINI_API_KEY: str = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL: str = os.environ.get("GEMINI_MODEL", "gemini-3.1-flash-live-preview")
