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

MINIAPP_CORS_ORIGINS: list[str] = [
    o.strip()
    for o in os.environ.get(
        "MINIAPP_CORS_ORIGINS",
        "https://web-app.telegram.org,https://tg.dev",
    ).split(",")
    if o.strip()
]

MINIAPP_API_PORT: int = int(os.environ.get("MINIAPP_API_PORT", "8081"))

# Gemini Live API
GEMINI_API_KEY: str = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL: str = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash-live")
